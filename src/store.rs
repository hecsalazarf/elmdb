use crate::transaction::{TransactionExt, TransactionRwExt};
use lmdb::{
  Cursor, Database, Environment, Error, Iter, Result, RwTransaction, Transaction, WriteFlags,
};
use serde::{Deserialize, Serialize};
use std::{borrow::Borrow, marker::PhantomData, ops::RangeBounds};

/// A typed key-value LMDB database.
#[derive(Copy, Clone, Debug)]
pub struct Store<D> {
  /// Inner database
  database: Database,
  _data: PhantomData<D>,
}

impl<D> Store<D> {
  /// Open a store with the provided database name.
  ///
  /// # Note
  /// Stores are not meant to be opened more than once, since there is no way
  /// (yet) to know if a store was previously operated on a different data type.
  /// Instead, copy the opened store whenever it's neded.
  pub fn open<K>(env: &Environment, db: K) -> Result<Self>
  where
    K: AsRef<str>,
  {
    let db_flags = lmdb::DatabaseFlags::default();
    let database = env.create_db(Some(db.as_ref()), db_flags)?;

    Ok(Self {
      database,
      _data: PhantomData,
    })
  }

  /// Insert data with key. Data can be any type that implements `serde:Serialize`.
  pub fn put<K, V: ?Sized>(&self, txn: &mut RwTransaction, key: K, value: &V) -> Result<()>
  where
    K: AsRef<[u8]>,
    D: Borrow<V>,
    V: Serialize,
  {
    txn.put_data(self.database, &key.as_ref(), value, WriteFlags::default())
  }

  /// Retrieve data from `Store` if keys exists.
  ///
  /// # Note
  /// This method returns `Error::Incompatible` if the stored value has a different type
  /// from the one that this method tried to deserialize.
  pub fn get<'txn, K, T>(&self, txn: &'txn T, key: K) -> Result<Option<D>>
  where
    K: AsRef<[u8]>,
    D: Deserialize<'txn>,
    T: Transaction,
  {
    txn.get_data(self.database, &key.as_ref())
  }

  /// Remove key from the database.
  pub fn remove<K>(&self, txn: &mut RwTransaction, key: K) -> Result<()>
  where
    K: AsRef<[u8]>,
  {
    txn.del(self.database, &key.as_ref(), None)
  }

  /// Remove keys within `range` from the database.
  pub fn remove_range<K, R>(&self, txn: &mut RwTransaction, range: R) -> Result<()>
  where
    K: AsRef<[u8]>,
    R: RangeBounds<K>,
  {
    let mut cursor = txn.open_rw_cursor(self.database)?;
    for res in cursor.iter_range(range) {
      res?;
      cursor.del(WriteFlags::default())?;
    }

    Ok(())
  }

  /// Returns the first key and value in the Store, or None if the Store is empty.
  pub fn first<'txn, T>(&self, txn: &'txn T) -> Result<Option<(&'txn [u8], D)>>
  where
    D: Deserialize<'txn>,
    T: Transaction,
  {
    self.iter(txn)?.take(1).next().transpose()
  }

  /// Returns the last key and value in the Store, or None if the Store is empty.
  pub fn last<'txn, T>(&self, txn: &'txn T) -> Result<Option<(&'txn [u8], D)>>
  where
    D: Deserialize<'txn>,
    T: Transaction,
  {
    self.iter_end_backwards(txn)?.take(1).next().transpose()
  }

  /// Returns `true` if the `Store` contains a value for the specified key.
  pub fn contains_key<K, T>(&self, txn: &T, key: K) -> Result<bool>
  where
    K: AsRef<[u8]>,
    T: Transaction,
  {
    txn
      .get_opt(self.database, &key.as_ref())
      .map(|opt| opt.is_some())
  }

  /// Returns an iterator over the whole database.
  pub fn iter<'txn, T>(&self, txn: &'txn T) -> Result<StoreIter<'txn, D>>
  where
    T: Transaction,
  {
    let mut cursor = txn.open_ro_cursor(self.database)?;
    Ok(StoreIter::new(cursor.iter_start()))
  }

  /// Returns an iterator positioned at first key greater than or equal to the specified key.
  pub fn iter_from<'txn, K, T>(&self, txn: &'txn T, key: K) -> Result<StoreIter<'txn, D>>
  where
    K: AsRef<[u8]>,
    T: Transaction,
  {
    let mut cursor = txn.open_ro_cursor(self.database)?;
    Ok(StoreIter::new(cursor.iter_from(key)))
  }

  /// Returns an iterator over specified `range`.
  pub fn range<'txn, T, K, R>(&self, txn: &'txn T, range: R) -> Result<StoreIter<'txn, D>>
  where
    T: Transaction,
    K: AsRef<[u8]>,
    R: RangeBounds<K>,
  {
    let mut cursor = txn.open_ro_cursor(self.database)?;
    Ok(StoreIter::new(cursor.iter_range(range)))
  }

  /// Returns an iterator positioned at the last key and iterating backwards.
  pub fn iter_end_backwards<'txn, T>(&self, txn: &'txn T) -> Result<StoreIter<'txn, D>>
  where
    T: Transaction,
  {
    let mut cursor = txn.open_ro_cursor(self.database)?;
    Ok(StoreIter::new(cursor.iter_end_backwards()))
  }

  /// Empties the store. All items will be removed.
  pub fn clear(&self, txn: &mut RwTransaction) -> Result<()> {
    txn.clear_db(self.database)
  }
}

/// Iterator over key/data pairs of a `Store`.
pub struct StoreIter<'txn, D> {
  inner: Iter<'txn>,
  _data: PhantomData<D>,
}

impl<'txn, D> StoreIter<'txn, D> {
  fn new(inner: Iter<'txn>) -> Self {
    Self {
      inner,
      _data: PhantomData,
    }
  }

  fn next_inner(&mut self) -> Option<Result<(&'txn [u8], D)>>
  where
    D: Deserialize<'txn>,
  {
    self.inner.next().and_then(|res| match res {
      Ok((key, val)) => {
        let v = bincode::deserialize(val);
        Some(v.map(|val| (key, val)).map_err(|_| Error::Incompatible))
      }
      Err(e) => Some(Err(e)),
    })
  }

  /// Iterate over the values of this `Store`.
  pub fn values(self) -> StoreValues<'txn, D> {
    StoreValues::new(self)
  }

  /// Iterate over the keys of this `Store`.
  pub fn keys(self) -> StoreKeys<'txn, D> {
    StoreKeys::new(self)
  }
}

impl<'txn, D: Deserialize<'txn>> Iterator for StoreIter<'txn, D> {
  type Item = Result<(&'txn [u8], D)>;

  fn next(&mut self) -> Option<Self::Item> {
    self.next_inner()
  }
}

/// An iterator that iterates the values of a `Store`.
pub struct StoreValues<'txn, D> {
  inner: StoreIter<'txn, D>,
}

impl<'txn, D> StoreValues<'txn, D> {
  fn new(inner: StoreIter<'txn, D>) -> Self {
    Self { inner }
  }
}

impl<'txn, D: Deserialize<'txn>> Iterator for StoreValues<'txn, D> {
  type Item = Result<D>;

  fn next(&mut self) -> Option<Self::Item> {
    self.inner.next().map(|res| res.map(|(_, v)| v))
  }
}

/// An iterator that iterates the keys of a `Store`.
pub struct StoreKeys<'txn, D> {
  inner: StoreIter<'txn, D>,
}

impl<'txn, D> StoreKeys<'txn, D> {
  fn new(inner: StoreIter<'txn, D>) -> Self {
    Self { inner }
  }
}

impl<'txn, D: Deserialize<'txn>> Iterator for StoreKeys<'txn, D> {
  type Item = Result<&'txn [u8]>;

  fn next(&mut self) -> Option<Self::Item> {
    self.inner.next().map(|res| res.map(|(k, _)| k))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::test_utils::create_env;

  #[test]
  fn put_get() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let store = Store::open(&env, "mystore")?;
    let mut tx = env.begin_rw_txn()?;
    store.put(&mut tx, "hello", "world")?;
    assert_eq!(Ok(Some("world".to_owned())), store.get(&mut tx, "hello"));

    Ok(())
  }

  #[test]
  fn iterators() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let store = Store::open(&env, "mystore")?;
    let items: Vec<(&[u8], u16)> = vec![(b"1", 1000), (b"2", 2000), (b"3", 3000), (b"5", 4000)];
    for (key, value) in items.iter() {
      let mut tx = env.begin_rw_txn()?;
      store.put(&mut tx, key, value)?;
      tx.commit()?;
    }

    let tx = env.begin_ro_txn()?;
    assert_eq!(items.clone(), store.iter(&tx)?.collect::<Result<Vec<_>>>()?);

    // Iterator from
    assert_eq!(
      items.clone().into_iter().skip(1).collect::<Vec<_>>(),
      store.iter_from(&tx, b"2")?.collect::<Result<Vec<_>>>()?
    );

    // Backwards iterator
    assert_eq!(
      items.clone().into_iter().rev().collect::<Vec<_>>(),
      store.iter_end_backwards(&tx)?.collect::<Result<Vec<_>>>()?
    );

    // Range iterator
    assert_eq!(
      items
        .clone()
        .into_iter()
        .skip(1)
        .take(2)
        .collect::<Vec<_>>(),
      store
        .range(&tx, &b"2"[..]..=b"3")?
        .collect::<Result<Vec<_>>>()?
    );

    // Values iterator
    assert_eq!(
      items
        .clone()
        .into_iter()
        .map(|(_, v)| v)
        .collect::<Vec<_>>(),
      store.iter(&tx)?.values().collect::<Result<Vec<_>>>()?
    );

    // Keys iterator
    assert_eq!(
      items
        .clone()
        .into_iter()
        .map(|(k, _)| k)
        .collect::<Vec<_>>(),
      store.iter(&tx)?.keys().collect::<Result<Vec<_>>>()?
    );

    Ok(())
  }

  #[test]
  fn get_incompatible() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let store: Store<bool> = Store::open(&env, "mystore")?;
    let mut tx = env.begin_rw_txn()?;
    store.put(&mut tx, "true", &true)?;
    tx.commit()?;

    let store = Store::<f64>::open(&env, "mystore")?;
    let tx = env.begin_ro_txn()?;
    // Err because we store a bool and we try to get a f64
    assert_eq!(Err(Error::Incompatible), store.get(&tx, "true"));
    Ok(())
  }

  #[test]
  fn first_last() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let store = Store::open(&env, "mystore")?;
    let tx = env.begin_ro_txn()?;
    assert_eq!(Ok(None), store.last(&tx));
    let tx = tx.reset().renew()?;
    assert_eq!(Ok(None), store.first(&tx));
    tx.abort();

    let items: Vec<(&[u8], u16)> = vec![(b"W", 2000), (b"X", 1000), (b"Y", 10), (b"Z", 100)];
    let mut tx = env.begin_rw_txn()?;
    for (key, value) in items.iter() {
      store.put(&mut tx, key, value)?;
    }
    tx.commit()?;

    let tx = env.begin_ro_txn()?;
    assert_eq!(Ok(items.clone().into_iter().last()), store.last(&tx));
    assert_eq!(Ok(items.clone().into_iter().nth(0)), store.first(&tx));
    Ok(())
  }

  #[test]
  fn remove() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let items: Vec<(&[u8], u16)> = vec![
      (b"V", 500),
      (b"W", 2000),
      (b"X", 1000),
      (b"Y", 10),
      (b"Z", 100),
    ];
    let store = Store::open(&env, "mystore")?;

    let mut tx = env.begin_rw_txn()?;
    for (key, value) in items.iter() {
      store.put(&mut tx, key, value)?;
    }
    tx.commit()?;

    // Remove single element
    let mut tx = env.begin_rw_txn()?;
    assert_eq!(Ok(()), store.remove(&mut tx, b"W"));
    tx.commit()?;

    let tx = env.begin_ro_txn()?;
    assert_eq!(
      items
        .clone()
        .into_iter()
        .filter(|(k, _)| { k != b"W" })
        .collect::<Vec<_>>(),
      store.iter(&tx)?.collect::<Result<Vec<_>>>()?
    );
    tx.abort();

    // Remove range
    let mut tx = env.begin_rw_txn()?;
    assert_eq!(Ok(()), store.remove_range(&mut tx, ..&b"Z"[..]));
    tx.commit()?;

    let tx = env.begin_ro_txn()?;
    assert_eq!(
      items.clone().into_iter().skip(4).collect::<Vec<_>>(),
      store.iter(&tx)?.collect::<Result<Vec<_>>>()?
    );
    tx.abort();
    Ok(())
  }

  #[test]
  fn contains_key() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let store: Store<String> = Store::open(&env, "mystore")?;
    let mut txn = env.begin_rw_txn()?;

    assert_eq!(Ok(false), store.contains_key(&txn, "key1"));
    store.put(&mut txn, "key1", "value1")?;
    assert_eq!(Ok(true), store.contains_key(&txn, "key1"));
    txn.commit()
  }

  #[test]
  fn clear() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let store: Store<String> = Store::open(&env, "mystore")?;
    let mut txn = env.begin_rw_txn()?;
    store.put(&mut txn, "key1", "value1")?;
    store.put(&mut txn, "key2", "value2")?;
    txn.commit()?;

    let mut txn = env.begin_rw_txn()?;
    store.clear(&mut txn)?;
    txn.commit()?;

    let txn = env.begin_ro_txn()?;
    assert_eq!(0, store.iter(&txn)?.count());

    Ok(())
  }
}
