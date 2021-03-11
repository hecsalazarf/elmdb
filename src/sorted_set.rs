//! A sorted set with persistent storage.
use crate::transaction::TransactionExt;
use lmdb::{Database, Environment, Iter, Result, RwTransaction, Transaction};
use std::ops::{Bound, RangeBounds};
use uuid::Uuid;

/// A sorted set with persistent storage.
///
/// Sorted sets are composed of unique, non-repeating elements, sorted by a `u64`
/// number called score.
#[derive(Clone, Debug)]
pub struct SortedSet {
  /// DB storing <uuid + score + element> encoded in the its key,
  /// serving as a skip list.
  skiplist: Database,
  /// DB mapping elements to their scores, simply a hash table.
  elements: Database,
  /// UUID v5 produced from sorted set key.
  uuid: Option<Uuid>,
}

impl SortedSet {
  /// UUID length.
  const UUID_LEN: usize = 16;
  /// Score length.
  const SCORE_LEN: usize = 8;
  /// Prefix length UUID + SCORE.
  const PREFIX_LEN_UUID: usize = Self::UUID_LEN + Self::SCORE_LEN;

  /// Create a new sorted set.
  pub(crate) fn new(skiplist: Database, elements: Database, uuid: Option<Uuid>) -> Self {
    Self {
      skiplist,
      elements,
      uuid,
    }
  }

  /// Add one element with the specified score. If specified element is already
  /// a member of the sorted set, the score is updated and the element reinserted
  /// at the right position to ensure the correct ordering.
  pub fn add<V>(&self, txn: &mut RwTransaction, score: u64, val: V) -> Result<()>
  where
    V: AsRef<[u8]>,
  {
    let mut txn = txn.begin_nested_txn()?;
    let encoded_element = self.encode_elements_key(val.as_ref());
    if let Some(old_score) = txn.get_opt(self.elements, &encoded_element)? {
      // If the member already exists, remove it from skiplist before new insertion
      let encoded_key = self.encode_skiplist_key(old_score, val.as_ref());
      txn.del(self.skiplist, &encoded_key, None)?;
    }

    let write_flags = lmdb::WriteFlags::default();
    let encoded_score = score.to_be_bytes();
    let encoded_key = self.encode_skiplist_key(&encoded_score, val.as_ref());
    // Insert new element into both skiplist and members databases
    txn.put(self.skiplist, &encoded_key, &[], write_flags)?;
    txn.put(self.elements, &encoded_element, &encoded_score, write_flags)?;
    txn.commit()
  }

  /// Return all the elements in the sorted set with a score between `range`.
  /// The elements are considered to be sorted from low to high scores.
  pub fn range_by_score<'txn, T, R>(&self, txn: &'txn T, range: R) -> Result<SortedRange<'txn>>
  where
    T: Transaction,
    R: RangeBounds<u64>,
  {
    use lmdb::Cursor;

    let (start, end) = self.to_bytes_range(range);
    let mut cursor = txn.open_ro_cursor(self.skiplist)?;
    let iter = cursor.iter_from(start);
    let has_uuid = self.uuid.is_some();

    Ok(SortedRange {
      end,
      iter,
      has_uuid,
    })
  }

  /// Remove the specified element from the sorted set, returning `true` when
  /// the member existed and was removed. If member is non-existant the result
  /// is `false`.
  pub fn remove<V>(&self, txn: &mut RwTransaction, val: V) -> Result<bool>
  where
    V: AsRef<[u8]>,
  {
    let encoded_element = self.encode_elements_key(val.as_ref());
    if let Some(score) = txn.get_opt(self.elements, &encoded_element)? {
      let encoded_key = self.encode_skiplist_key(score, val.as_ref());
      let mut txn = txn.begin_nested_txn()?;
      txn.del(self.skiplist, &encoded_key, None)?;
      txn.del(self.elements, &encoded_element, None)?;
      txn.commit()?;
      return Ok(true);
    }
    Ok(false)
  }

  /// Remove all elements in the sorted set stored with a score between `range`,
  /// returning the number of elements removed.
  pub fn remove_range_by_score<R>(&self, txn: &mut RwTransaction, range: R) -> Result<usize>
  where
    R: RangeBounds<u64>,
  {
    let mut txn = txn.begin_nested_txn()?;
    let mut range = self.range_by_score(&txn, range)?;
    let mut removed = 0;
    while let Some(key) = range.next_inner().transpose()? {
      let encoded_element = if self.uuid.is_some() {
        self.encode_elements_key(&key[Self::PREFIX_LEN_UUID..])
      } else {
        self.encode_elements_key(&key[Self::SCORE_LEN..])
      };

      txn.del(self.skiplist, &key, None)?;
      txn.del(self.elements, &encoded_element, None)?;
      removed += 1;
    }
    txn.commit()?;
    Ok(removed)
  }

  /// Returns the score associated with the specified element at `val` in the sorted set.
  /// If the element does not exist, `None` is returned.
  pub fn score<T, V>(&self, txn: &T, val: V) -> Result<Option<u64>>
  where
    T: Transaction,
    V: AsRef<[u8]>,
  {
    let encoded_element = self.encode_elements_key(val.as_ref());
    let element = txn.get_opt(self.elements, encoded_element)?;
    Ok(element.map(|el| {
      use std::convert::TryInto;
      let score: [u8; Self::SCORE_LEN] = el.try_into().expect("score as array");
      u64::from_be_bytes(score)
    }))
  }

  fn encode_elements_key(&self, val: &[u8]) -> Vec<u8> {
    let mut key = self.key_as_vec();
    key.extend(val);
    key
  }

  fn encode_skiplist_key(&self, score: &[u8], val: &[u8]) -> Vec<u8> {
    let mut key = self.key_as_vec();
    key.extend(score);
    key.extend(val);
    key
  }

  fn key_as_vec(&self) -> Vec<u8> {
    self
      .uuid
      .as_ref()
      .map(|u| u.as_bytes().to_vec())
      .unwrap_or(Vec::new())
  }

  fn to_bytes_range<R>(&self, range: R) -> (BoundLimit, Bound<BoundLimit>)
  where
    R: RangeBounds<u64>,
  {
    let uuid_bytes = self.uuid.as_ref().map(|u| &u.as_bytes()[..]);
    let start = match range.start_bound() {
      Bound::Excluded(score) => {
        // Increment one to exclude the start range
        // TODO Analyze edge case when score is u64::MAX. Such case should
        // return an empty interator
        Self::create_bound_value(uuid_bytes, &score.saturating_add(1))
      }
      Bound::Included(score) => {
        // Included bound
        Self::create_bound_value(uuid_bytes, score)
      }
      Bound::Unbounded => {
        // Unbounded start has zeroed score
        Self::create_bound_value(uuid_bytes, &u64::MIN)
      }
    };

    let end = match range.end_bound() {
      Bound::Excluded(score) => {
        let bound = Self::create_bound_value(uuid_bytes, score);
        Bound::Excluded(bound)
      }
      Bound::Included(score) => {
        let bound = Self::create_bound_value(uuid_bytes, score);
        Bound::Included(bound)
      }
      Bound::Unbounded => self.create_unbounded_bound(),
    };

    (start, end)
  }

  fn create_unbounded_bound(&self) -> Bound<BoundLimit> {
    // The unbounded range from user's perspective encompases all the elements with the
    // same key. So we create the upper limit as Bound::Excluded((UUID + 1 ) + MIN_SCORE)
    if let Some(ref uuid) = self.uuid {
      let uiid_num = u128::from_be_bytes(*uuid.as_bytes());
      if let Some(next_uuid) = uiid_num.checked_add(1) {
        let bound = Self::create_bound_value(Some(&next_uuid.to_be_bytes()), &u64::MIN);
        return Bound::Excluded(bound);
      }
    }

    // However, if the UUID overflows, we reached the maximum UUID. Only
    // such case means an iteration until the database end. Unlikely, yes;
    // but theoretically possible
    Bound::Unbounded
  }

  fn create_bound_value(uuid: Option<&[u8]>, score: &u64) -> BoundLimit {
    use std::convert::TryInto;

    if let Some(u) = uuid {
      let uuid_slice: &[u8; Self::UUID_LEN] = u.try_into().expect("uuid into array");
      let score_bytes = score.to_be_bytes();

      // Chain uuid with score
      let chain = uuid_slice.iter().chain(&score_bytes);
      let mut limit = [0; Self::PREFIX_LEN_UUID];
      // Copy chain to the limit array
      limit
        .iter_mut()
        .zip(chain)
        .for_each(|(new, chained)| *new = *chained);
      BoundLimit::Id(limit)
    } else {
      BoundLimit::Score(score.to_be_bytes())
    }
  }
}

#[derive(Debug)]
enum BoundLimit {
  Id([u8; SortedSet::PREFIX_LEN_UUID]),
  Score([u8; SortedSet::SCORE_LEN]),
}

impl AsRef<[u8]> for BoundLimit {
  fn as_ref(&self) -> &[u8] {
    match self {
      Self::Id(val) => &val[..],
      Self::Score(val) => &val[..],
    }
  }
}

/// Iterator with elements returned after calling `SortedSet::range_by_score`.
#[derive(Debug)]
pub struct SortedRange<'txn> {
  end: Bound<BoundLimit>,
  iter: Iter<'txn>,
  has_uuid: bool,
}

impl<'txn> SortedRange<'txn> {
  fn next_inner(&mut self) -> Option<Result<&'txn [u8]>> {
    let res = self.iter.next()?;
    if let Err(e) = res {
      return Some(Err(e));
    }

    let key = res.unwrap().0;
    let prefix_key = if self.has_uuid {
      &key[..SortedSet::PREFIX_LEN_UUID]
    } else {
      &key[..SortedSet::SCORE_LEN]
    };

    let is_in_range = match &self.end {
      Bound::Excluded(k) => prefix_key < k.as_ref(),
      Bound::Included(k) => prefix_key <= k.as_ref(),
      Bound::Unbounded => true, // Return all elements until the end
    };

    if is_in_range {
      Some(Ok(key))
    } else {
      None
    }
  }
}

impl<'txn> Iterator for SortedRange<'txn> {
  type Item = Result<&'txn [u8]>;

  fn next(&mut self) -> Option<Self::Item> {
    self
      .next_inner()
      // Include only the element in the returned item
      .map(|res| {
        res.map(|key| {
          if self.has_uuid {
            &key[SortedSet::PREFIX_LEN_UUID..]
          } else {
            &key[SortedSet::SCORE_LEN..]
          }
        })
      })
  }
}

/// Sorted sets handler in a dedicated LMDB database
#[derive(Debug, Clone)]
pub struct SortedSetDb {
  skiplist: Database,
  elements: Database,
}

impl SortedSetDb {
  /// Skiplist DB name.
  const SKIPLIST_DB_NAME: &'static str = "__sorted_set_skiplist";
  /// Members DB name.
  const ELEMENTS_DB_NAME: &'static str = "__sorted_set_elements";
  /// Open a database of sorted sets with the given `name`. If `name` is `None`, the
  /// default detabase is used.
  pub fn open(env: &Environment, name: Option<&str>) -> Result<Self> {
    let (skiplist, elements) = Self::create_dbs(env, name)?;
    Ok(Self { skiplist, elements })
  }

  /// Get the sorted set with the specified key.
  pub fn get<K: AsRef<[u8]>>(&self, key: K) -> SortedSet {
    let skiplist = self.skiplist;
    let elements = self.elements;
    let uuid = Some(Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_ref()));

    SortedSet::new(skiplist, elements, uuid)
  }

  /// Create DB's needed by a sorted set
  pub(crate) fn create_dbs(env: &Environment, name: Option<&str>) -> Result<(Database, Database)> {
    let (sl, el) = name
      .map(|n| {
        (
          format!("{}_{}", Self::SKIPLIST_DB_NAME, n),
          format!("{}_{}", Self::ELEMENTS_DB_NAME, n),
        )
      })
      .unwrap_or((Self::SKIPLIST_DB_NAME.into(), Self::ELEMENTS_DB_NAME.into()));

    let db_flags = lmdb::DatabaseFlags::default();
    let skiplist = env.create_db(Some(&sl), db_flags)?;
    let elements = env.create_db(Some(&el), db_flags)?;

    Ok((skiplist, elements))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::test_utils::{create_env, utf8_to_str};
  use lmdb::Cursor;

  #[test]
  fn default_dbs() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let db_flags = lmdb::DatabaseFlags::default();

    let (sl, el) = SortedSetDb::create_dbs(&env, None)?;
    assert_eq!(
      sl,
      env.create_db(Some(SortedSetDb::SKIPLIST_DB_NAME), db_flags)?
    );
    assert_eq!(
      el,
      env.create_db(Some(SortedSetDb::ELEMENTS_DB_NAME), db_flags)?
    );

    Ok(())
  }

  #[test]
  fn range_by_score() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let set_db = SortedSetDb::open(&env, None)?;
    let set_a = set_db.get("set_a");

    // Add to set
    let mut tx = env.begin_rw_txn().expect("rw txn");
    set_a.add(&mut tx, 100, "Elephant")?;
    set_a.add(&mut tx, 50, "Bear")?;
    set_a.add(&mut tx, 20, "Cat")?;
    set_a.add(&mut tx, 101, "Bigger Elephant")?;
    tx.commit()?;

    // Get a subset
    let tx = env.begin_ro_txn().expect("ro txn");
    let mut range = set_a.range_by_score(&tx, 20..=50)?;
    assert_eq!(range.next().map(utf8_to_str), Some(Ok("Cat")));
    assert_eq!(range.next().map(utf8_to_str), Some(Ok("Bear")));
    assert_eq!(range.next().map(utf8_to_str), None);

    // Exclude last member
    let tx = tx.reset().renew()?;
    let mut range = set_a.range_by_score(&tx, 100..101)?;
    assert_eq!(range.next().map(utf8_to_str), Some(Ok("Elephant")));
    assert_eq!(range.next(), None);

    // Include last member
    let tx = tx.reset().renew()?;
    let mut range = set_a.range_by_score(&tx, 100..=101)?;
    assert_eq!(range.next().map(utf8_to_str), Some(Ok("Elephant")));
    assert_eq!(range.next().map(utf8_to_str), Some(Ok("Bigger Elephant")));
    assert_eq!(range.next(), None);
    Ok(())
  }

  #[test]
  fn range_by_score_unbounded() -> Result<()> {
    // UUID_A = UUID::MAX - 1
    const UUID_A: [u8; 16] = [
      0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
      0xfe,
    ];
    // UUID_B = UUID::MAX
    const UUID_B: [u8; 16] = [0xff; 16];

    let (_tmpdir, env) = create_env()?;
    let set_db = SortedSetDb::open(&env, None)?;
    let mut set_a = set_db.get("set_a");
    let mut set_b = set_db.get("set_b");
    // Intentionally change uuid to test edge case
    set_a.uuid = Some(Uuid::from_slice(&UUID_A).unwrap());
    set_b.uuid = Some(Uuid::from_slice(&UUID_B).unwrap());

    let mut tx = env.begin_rw_txn().expect("rw txn");
    set_a.add(&mut tx, 100, "Elephant")?;
    set_a.add(&mut tx, 50, "Bear")?;
    set_b.add(&mut tx, 10, "Cat")?;
    tx.commit()?;

    // Set A with UUID_A does not overlap with UUID_B
    let tx = env.begin_ro_txn().expect("ro txn");
    let mut range = set_a.range_by_score(&tx, ..)?;
    assert_eq!(range.next().map(utf8_to_str), Some(Ok("Bear")));
    assert_eq!(range.next().map(utf8_to_str), Some(Ok("Elephant")));
    assert_eq!(range.next(), None);

    // Set B upper limit is the end of database
    let tx = tx.reset().renew()?;
    let mut range = set_b.range_by_score(&tx, ..)?;
    assert_eq!(range.next().map(utf8_to_str), Some(Ok("Cat")));
    assert_eq!(range.next(), None);
    Ok(())
  }

  #[test]
  fn unique_member() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let set_db = SortedSetDb::open(&env, None)?;
    let set_a = set_db.get("set_a");
    let mut tx = env.begin_rw_txn().expect("rw txn");
    set_a.add(&mut tx, 100, "Elephant")?;
    // Update the same member with a different score
    set_a.add(&mut tx, 2000, "Elephant")?;
    tx.commit()?;

    // Get the whole set
    let tx = env.begin_ro_txn().expect("ro txn");
    let mut range = set_a.range_by_score(&tx, ..)?;
    assert_eq!(range.next().map(utf8_to_str), Some(Ok("Elephant")));
    assert_eq!(range.next(), None);
    Ok(())
  }

  #[test]
  fn same_score_diff_member() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let set_db = SortedSetDb::open(&env, None)?;
    let set_a = set_db.get("set_a");
    let mut tx = env.begin_rw_txn().expect("rw txn");
    set_a.add(&mut tx, 100, "Asian Elephant")?;
    // Add new member with the same score
    set_a.add(&mut tx, 100, "African Elephant")?;
    tx.commit()?;

    // Get the whole set
    let tx = env.begin_ro_txn().expect("ro txn");
    let mut range = set_a.range_by_score(&tx, ..)?;
    assert_eq!(range.next().map(utf8_to_str), Some(Ok("African Elephant")));
    assert_eq!(range.next().map(utf8_to_str), Some(Ok("Asian Elephant")));
    assert_eq!(range.next(), None);
    Ok(())
  }

  #[test]
  fn remove_element() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let set_db = SortedSetDb::open(&env, None)?;
    let set_a = set_db.get("set_a");

    let mut tx = env.begin_rw_txn().expect("rw txn");
    set_a.add(&mut tx, 2000, "Elephant")?;
    assert_eq!(Ok(true), set_a.remove(&mut tx, "Elephant"));
    assert_eq!(Ok(false), set_a.remove(&mut tx, "Elephant"));
    Ok(())
  }

  #[test]
  fn remove_range_by_score() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let set_db = SortedSetDb::open(&env, None)?;
    let set_a = set_db.get("set_a");

    let mut tx = env.begin_rw_txn().expect("rw txn");
    set_a.add(&mut tx, 100, "Elephant")?;
    set_a.add(&mut tx, 50, "Bear")?;
    set_a.add(&mut tx, 20, "Cat")?;

    // Remove the first two elements
    assert_eq!(Ok(2), set_a.remove_range_by_score(&mut tx, 20..=50));
    // Remove left elements
    assert_eq!(Ok(1), set_a.remove_range_by_score(&mut tx, ..));
    tx.commit()?;

    // Check that elements DB is empty
    let tx = env.begin_ro_txn().expect("ro txn");
    let mut iter = tx.open_ro_cursor(set_a.elements)?.iter_start();
    assert_eq!(None, iter.next());

    // Check that skiplist DB is empty
    let tx = tx.reset().renew()?;
    let mut iter = tx.open_ro_cursor(set_a.skiplist)?.iter_start();
    assert_eq!(None, iter.next());
    Ok(())
  }

  #[test]
  fn score() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let set_db = SortedSetDb::open(&env, None)?;
    let set_a = set_db.get("set_a");
    let txn = env.begin_ro_txn()?;
    assert_eq!(Ok(None), set_a.score(&txn, "Popocatepetl"));

    let mut txn = env.begin_rw_txn()?;
    set_a.add(&mut txn, 5_426, "Popocatepetl")?;
    set_a.add(&mut txn, 5_230, "Iztaccihuatl")?;
    txn.commit()?;

    let txn = env.begin_ro_txn()?;
    assert_eq!(Ok(Some(5_426)), set_a.score(&txn, "Popocatepetl"));
    Ok(())
  }
}
