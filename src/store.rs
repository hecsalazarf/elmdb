use crate::transaction::{TransactionExt, TransactionRwExt};
use lmdb::{
  Cursor, Database, Environment, Error, Iter, Result, RwTransaction, Transaction, WriteFlags,
};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

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
  pub fn put<K>(&self, txn: &mut RwTransaction, key: K, data: &D) -> Result<()>
  where
    K: AsRef<[u8]>,
    D: Serialize,
  {
    txn.put_data(self.database, &key.as_ref(), &data, WriteFlags::default())
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
  pub fn delete<K>(&self, txn: &mut RwTransaction, key: K) -> Result<()>
  where
    K: AsRef<[u8]>,
  {
    txn.del(self.database, &key.as_ref(), None)
  }

  /// Returns the first key and value in the Store, or None if the Store is empty.
  pub fn first<'txn, T>(&self, txn: &'txn T) -> Result<Option<(&'txn [u8], D)>>
  where
    D: Deserialize<'txn>,
    T: Transaction,
  {
    self.iter_from(txn, [0])?.take(1).next().transpose()
  }

  /// Returns the last key and value in the Store, or None if the Store is empty.
  pub fn last<'txn, T>(&self, txn: &'txn T) -> Result<Option<(&'txn [u8], D)>>
  where
    D: Deserialize<'txn>,
    T: Transaction,
  {
    self.iter_end_backwards(txn)?.take(1).next().transpose()
  }

  /// Returns an iterator positioned at first key greater than or equal to the specified key.
  pub fn iter_from<'txn, K, T>(&self, txn: &'txn T, key: K) -> Result<StoreIter<'txn, D>>
  where
    K: AsRef<[u8]>,
    T: Transaction,
  {
    let mut cursor = txn.open_ro_cursor(self.database)?;
    let iter = cursor.iter_from(key);
    Ok(StoreIter::new(iter))
  }

  /// Returns an iterator positioned at the last key and iterating backwards.
  pub fn iter_end_backwards<'txn, T>(&self, txn: &'txn T) -> Result<StoreIter<'txn, D>>
  where
    T: Transaction,
  {
    let mut cursor = txn.open_ro_cursor(self.database)?;
    let iter = cursor.iter_end_backwards();
    Ok(StoreIter::new(iter))
  }
}

/// Iterator over key/data pairs of a store.
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
}

impl<'txn, D: Deserialize<'txn>> Iterator for StoreIter<'txn, D> {
  type Item = Result<(&'txn [u8], D)>;

  fn next(&mut self) -> Option<Self::Item> {
    self.next_inner()
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
    store.put(&mut tx, "hello", &"world")?;
    assert_eq!(Ok(Some("world")), store.get(&mut tx, "hello"));

    Ok(())
  }

  #[test]
  fn iterators() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let store = Store::open(&env, "mystore")?;
    let items: Vec<(&[u8], u16)> = vec![(b"0", 10), (b"1", 100), (b"2", 1000)];
    for (key, value) in items.iter() {
      let mut tx = env.begin_rw_txn()?;
      store.put(&mut tx, key, value)?;
      tx.commit()?;
    }

    let tx = env.begin_ro_txn()?;
    let iter = store.iter_from(&tx, [0])?;
    assert_eq!(items, iter.collect::<Result<Vec<_>>>()?);
    let tx = tx.reset().renew()?;
    let iter_back = store.iter_end_backwards(&tx)?;
    let back_items: Vec<_> = items.into_iter().rev().collect();
    assert_eq!(back_items, iter_back.collect::<Result<Vec<_>>>()?);
    Ok(())
  }

  #[test]
  fn get_incompatible() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let store = Store::open(&env, "mystore")?;
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

    let items: Vec<(&[u8], u16)> = vec![(b"Z", 10), (b"Y", 100), (b"X", 1000)];
    for (key, value) in items.iter() {
      let mut tx = env.begin_rw_txn()?;
      store.put(&mut tx, key, value)?;
      tx.commit()?;
    }

    let tx = env.begin_ro_txn()?;
    assert_eq!(Ok(Some((&b"Z"[..], 10))), store.last(&tx));
    let tx = tx.reset().renew()?;
    assert_eq!(Ok(Some((&b"X"[..], 1000))), store.first(&tx));
    Ok(())
  }
}
