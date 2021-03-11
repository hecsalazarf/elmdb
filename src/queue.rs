//! A queue with persitent storage.
use crate::transaction::{TransactionExt, TransactionRwExt};
use lmdb::{
  Cursor, Database, Environment, Error, Iter, Result, RwTransaction, Transaction, WriteFlags,
};
use uuid::Uuid;

type MetaKey = [u8; 17];
type ElementKey = [u8; 24];

#[derive(Clone, Debug)]
struct QueueKeys {
  /// Queue's tail.
  tail: MetaKey,
}

impl QueueKeys {
  const TAIL_SUFFIX: &'static [u8] = &[0];

  fn new(uuid: &Uuid) -> Self {
    let uuid_bytes = uuid.as_bytes();
    let tail = Self::create_key(uuid_bytes, Self::TAIL_SUFFIX);

    Self { tail }
  }

  fn create_key(uuid: &[u8; 16], suffix: &[u8]) -> MetaKey {
    let mut key = MetaKey::default();
    let head_chain = uuid.iter().chain(suffix);
    key
      .iter_mut()
      .zip(head_chain)
      .for_each(|(new, chained)| *new = *chained);

    key
  }
}

/// A queue with persitent storage.
///
/// It is a collection of elements sorted according to the order of insertion, also
/// know as linked list. Call [`push`] to add elements to the tail, and [`pop`] to
/// remove from the head.
///
/// [`push`]: Queue::push
/// [`pop`]: Queue::pop
#[derive(Clone, Debug)]
pub struct Queue {
  /// Grouped keys for meta retrieval
  keys: QueueKeys,
  /// Elements (members)
  elements: Database,
  /// Metadata DB, containing the head and tail values
  meta: Database,
  /// UUID
  uuid: Uuid,
  subscriptions: &'static QueueSubcriptions,
}

impl Queue {
  /// Preppend one element to the queue's tail.
  pub fn push<V: AsRef<[u8]>>(&self, txn: &mut RwTransaction, val: V) -> Result<()> {
    let mut txn = txn.begin_nested_txn()?;
    let write_flags = WriteFlags::default();

    let (next_tail, _) = txn.incr_by(self.meta, self.keys.tail, 1, write_flags)?;
    let encoded_key = self.encode_members_key(&next_tail.to_be_bytes());
    if txn.get_opt(self.elements, encoded_key)?.is_some() {
      // Error if full, transaction must abort
      return Err(Error::KeyExist);
    }

    txn.put(self.elements, &encoded_key, &val, write_flags)?;
    txn.commit()
  }

  /// Pop one element from the queue's head. Return `None` if queue is empty.
  pub fn pop<'txn>(&self, txn: &'txn mut RwTransaction) -> Result<Option<&'txn [u8]>> {
    let mut txn = txn.begin_nested_txn()?;
    let mut cursor = txn.open_rw_cursor(self.elements)?;
    let iter = self.iter_from_cursor(&mut cursor);
    let opt_pop = iter.take(1).next().transpose()?;

    if opt_pop.is_some() {
      let write_flags = WriteFlags::default();
      cursor.del(write_flags)?;
    }
    drop(cursor);
    txn.commit()?;

    Ok(opt_pop)
  }

  /// Remove the first `count` elements equal to `val` from the queue.
  pub fn remove<V>(&self, txn: &mut RwTransaction, count: usize, val: V) -> Result<usize>
  where
    V: AsRef<[u8]>,
  {
    let mut txn = txn.begin_nested_txn()?;
    let mut iter = self.iter(&txn)?;
    let val = val.as_ref();
    let mut removed = 0;

    while let Some((key, value)) = iter.next_inner().transpose()? {
      if val == value {
        txn.del(self.elements, &key, None)?;
        removed += 1;
        if removed == count {
          break;
        }
      }
    }
    txn.commit()?;
    Ok(removed)
  }

  /// Create an iterator over the elements of the queue.
  pub fn iter<'txn, T>(&self, txn: &'txn T) -> Result<QueueIter<'txn, '_>>
  where
    T: Transaction,
  {
    let mut cursor = txn.open_ro_cursor(self.elements)?;
    let iter = self.iter_from_cursor(&mut cursor);
    Ok(iter)
  }

  /// Returns the element at `index`. The index is zero-based, so 0 means the first
  /// element, 1 the second element and so on.
  pub fn get<'txn, T>(&self, txn: &'txn T, index: usize) -> Result<Option<&'txn [u8]>>
  where
    T: Transaction,
  {
    let mut cursor = txn.open_ro_cursor(self.elements)?;
    let iter = self.iter_from_cursor(&mut cursor);
    iter.skip(index).take(1).next().transpose()
  }

  /// Returns and removes the head element of the queue, and pushes the element
  /// at the tail of destination.
  pub fn pop_and_move<'txn>(
    &self,
    txn: &'txn mut RwTransaction,
    dest: &Self,
  ) -> Result<Option<Vec<u8>>> {
    if let Some(elm) = self.pop(txn)? {
      // Hate to copy, but rust does not allow to pass txn as mutable twice
      // when holding the reference to elm.
      let copy = elm.to_vec();
      dest.push(txn, &copy)?;
      return Ok(Some(copy));
    }

    Ok(None)
  }

  /// Create a new subscriber that listens to events on this queue.
  pub async fn subscribe(&self) -> QueueSubscriber {
    self.subscriptions.subscriber(&self.uuid).await
  }

  /// Publish an `event` to the queue's channel.
  pub async fn publish(&self, evt: QueueEvent) {
    self.subscriptions.publish(&self.uuid, evt).await
  }

  fn encode_members_key(&self, index: &[u8]) -> ElementKey {
    let chain = self.uuid.as_bytes().iter().chain(index);
    let mut key = ElementKey::default();

    key
      .iter_mut()
      .zip(chain)
      .for_each(|(new, chained)| *new = *chained);
    key
  }

  pub fn iter_from_cursor<'txn, C>(&self, cursor: &mut C) -> QueueIter<'txn, '_>
  where
    C: Cursor<'txn>,
  {
    QueueIter {
      inner: cursor.iter_from(self.uuid.as_bytes()),
      uuid: &self.uuid,
    }
  }
}

/// Iterator on queue's elements.
pub struct QueueIter<'txn, 'q> {
  inner: Iter<'txn>,
  uuid: &'q Uuid,
}

impl<'txn> QueueIter<'txn, '_> {
  fn next_inner(&mut self) -> Option<Result<(&'txn [u8], &'txn [u8])>> {
    let next = self.inner.next();
    if let Some(Ok((key, _))) = next {
      // Extract the uuid and compare it, so that we know we are still
      // on the same queue
      let uuid_bytes = self.uuid.as_bytes();
      if &key[..uuid_bytes.len()] == uuid_bytes {
        next
      } else {
        // Different uuid, iterator is over
        None
      }
    } else {
      next
    }
  }
}

impl<'txn> Iterator for QueueIter<'txn, '_> {
  type Item = Result<&'txn [u8]>;

  fn next(&mut self) -> Option<Self::Item> {
    self.next_inner().map(|res| res.map(|(_, val)| val))
  }
}

/// Queues handler in a dedicated LMDB database.
#[derive(Debug, Clone)]
pub struct QueueDb {
  elements: Database,
  meta: Database,
}

impl QueueDb {
  /// Meta DB name.
  const META_DB_NAME: &'static str = "__queue_meta";
  /// Elements DB name.
  const ELEMENTS_DB_NAME: &'static str = "__queue_elements";

  /// Open a database of queues with the given `name`. If `name` is `None`, the
  /// default detabase is used.
  pub fn open(env: &Environment, name: Option<&str>) -> Result<Self> {
    let (meta, elements) = Self::create_dbs(env, name)?;
    Ok(Self { elements, meta })
  }

  /// Get a queue with the provided key.
  pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Queue {
    let uuid = Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_ref());
    let keys = QueueKeys::new(&uuid);
    let elements = self.elements;
    let meta = self.meta;
    let subscriptions = QueueSubcriptions::get_or_init();

    Queue {
      elements,
      meta,
      uuid,
      keys,
      subscriptions,
    }
  }

  /// Create DB's needed by a queue
  pub(crate) fn create_dbs(env: &Environment, name: Option<&str>) -> Result<(Database, Database)> {
    let (mt, el) = name
      .map(|n| {
        (
          format!("{}_{}", Self::META_DB_NAME, n),
          format!("{}_{}", Self::ELEMENTS_DB_NAME, n),
        )
      })
      .unwrap_or((Self::META_DB_NAME.into(), Self::ELEMENTS_DB_NAME.into()));

    let db_flags = lmdb::DatabaseFlags::default();
    let meta = env.create_db(Some(&mt), db_flags)?;
    let elements = env.create_db(Some(&el), db_flags)?;

    Ok((meta, elements))
  }
}

use std::collections::HashMap;
use std::sync::Once;
use tokio::sync::broadcast::{self, Receiver, Sender};
use tokio::sync::RwLock;

/// Singleton to manage the channels that propagate 'QueueEvent's.
#[derive(Debug)]
struct QueueSubcriptions {
  subs: RwLock<HashMap<Uuid, Sender<QueueEvent>>>,
}

impl QueueSubcriptions {
  // Get or init the `QueueSubcriptions` singleton.
  pub fn get_or_init() -> &'static Self {
    static START: Once = Once::new();
    static mut QUEUE_SUBS: Option<QueueSubcriptions> = None;

    unsafe {
      // Safe bacause mutation is made only once in a synchronized fashion
      START.call_once(|| {
        let instance = QueueSubcriptions {
          subs: RwLock::new(HashMap::new()),
        };
        QUEUE_SUBS = Some(instance);
      });

      QUEUE_SUBS.as_ref().unwrap()
    }
  }

  /// Create a new subscriber for the queue with given `uuid`.
  pub async fn subscriber(&self, uuid: &Uuid) -> QueueSubscriber {
    let subs = self.subs.read().await;
    let rx = if let Some(tx) = subs.get(uuid) {
      tx.subscribe()
    } else {
      drop(subs);
      let mut subs = self.subs.write().await;
      let (tx, rx) = broadcast::channel(1);
      subs.insert(*uuid, tx);
      rx
    };

    QueueSubscriber { rx }
  }

  /// Publish an `event` to the queue channel with `uuid`.
  pub async fn publish(&self, uuid: &Uuid, event: QueueEvent) -> () {
    let subs = self.subs.read().await;
    if let Some(tx) = subs.get(&uuid) {
      if let Err(_) = tx.send(event) {
        // TODO: Implement Drop for QueueSubscriber to remove the sender if
        // there are no more receivers, and avoid this on-demand removal.

        // If all receivers have been dropped, remove the sender
        // As there is an active reader, we drop it to avoid lock
        drop(subs);
        let mut subs = self.subs.write().await;
        subs.remove(uuid);
      }
    }
  }
}

/// A subscriber for queue events.
#[derive(Debug)]
pub struct QueueSubscriber {
  rx: Receiver<QueueEvent>,
}

impl QueueSubscriber {
  /// Waits to receive an event for the queue. Only push events in the meantime.
  pub async fn recv(&mut self) -> QueueEvent {
    loop {
      if let Ok(evt) = self.rx.recv().await {
        return evt;
      }
      // Errors are not handled because when response is `RecvError::Lagged`, we
      // keep on waiting for new messages. Besides, `RecvError::Closed` shall not
      // happen since sender is kept active while there are still receivers.
    }
  }
}

/// Events on queues.
#[derive(PartialEq, Copy, Clone, Debug)]
pub enum QueueEvent {
  /// A new element has been pushed to the queue.
  Push,
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::test_utils::{create_env, utf8_to_str};
  use lmdb::Transaction;

  #[test]
  fn default_dbs() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let db_flags = lmdb::DatabaseFlags::default();

    let (mt, el) = QueueDb::create_dbs(&env, None)?;
    assert_eq!(mt, env.create_db(Some(QueueDb::META_DB_NAME), db_flags)?);
    assert_eq!(
      el,
      env.create_db(Some(QueueDb::ELEMENTS_DB_NAME), db_flags)?
    );

    Ok(())
  }

  #[test]
  fn push() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let queue_db = QueueDb::open(&env, None)?;
    let queue_1 = queue_db.get("myqueue");
    let queue_2 = queue_db.get("anotherqueue");

    let mut tx = env.begin_rw_txn()?;
    queue_1.push(&mut tx, "Y")?;
    queue_1.push(&mut tx, "Z")?;
    queue_1.push(&mut tx, "X")?;
    tx.commit()?;

    let tx = env.begin_ro_txn()?;
    let mut iter = queue_1.iter(&tx)?;
    assert_eq!(Some(Ok("Y")), iter.next().map(utf8_to_str));
    assert_eq!(Some(Ok("Z")), iter.next().map(utf8_to_str));
    assert_eq!(Some(Ok("X")), iter.next().map(utf8_to_str));
    assert_eq!(None, iter.next().map(utf8_to_str));
    tx.commit()?;

    let mut tx = env.begin_rw_txn()?;
    queue_2.push(&mut tx, "A")?;
    tx.commit()?;

    let tx = env.begin_ro_txn()?;
    let mut iter = queue_2.iter(&tx)?;
    assert_eq!(Some(Ok("A")), iter.next().map(utf8_to_str));
    assert_eq!(None, iter.next().map(utf8_to_str));
    tx.commit()
  }

  #[test]
  fn push_full_queue() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let queue_db = QueueDb::open(&env, None)?;
    let queue = queue_db.get("myqueue");
    let mut tx = env.begin_rw_txn()?;
    queue.push(&mut tx, "X")?;
    // Increment the index to set it back to zero
    tx.incr_by(
      queue.meta,
      queue.keys.tail,
      u64::MAX - 1,
      WriteFlags::default(),
    )?;
    tx.commit()?;

    let mut tx = env.begin_rw_txn()?;
    // Ok because zero position is empty
    assert_eq!(Ok(()), queue.push(&mut tx, "Y"));
    // Err because the first push appended a value at index 1
    assert_eq!(Err(Error::KeyExist), queue.push(&mut tx, "Z"));
    Ok(())
  }

  #[test]
  fn pop() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let queue_db = QueueDb::open(&env, None)?;
    let queue_1 = queue_db.get("myqueue_waiting");
    let queue_2 = queue_db.get("myqueue_inprocess");

    let mut tx = env.begin_rw_txn()?;
    queue_1.push(&mut tx, "Y")?;
    queue_1.push(&mut tx, "Z")?;
    queue_2.push(&mut tx, "A")?;

    let opt_pop = queue_1.pop(&mut tx).transpose();
    assert_eq!(Some(Ok("Y")), opt_pop.map(utf8_to_str));
    let opt_pop = queue_1.pop(&mut tx).transpose();
    assert_eq!(Some(Ok("Z")), opt_pop.map(utf8_to_str));

    let opt_pop = queue_1.pop(&mut tx).transpose();
    assert_eq!(None, opt_pop.map(utf8_to_str));
    tx.commit()
  }

  #[test]
  fn remove() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let queue_db = QueueDb::open(&env, None)?;
    let queue = queue_db.get("myqueue");
    let mut tx = env.begin_rw_txn()?;
    queue.push(&mut tx, "X")?;
    queue.push(&mut tx, "X")?;
    queue.push(&mut tx, "Y")?;

    let removed = queue.remove(&mut tx, 2, "X")?;
    assert_eq!(2, removed);
    tx.commit()?;

    let tx = env.begin_ro_txn()?;
    let mut iter = queue.iter(&tx)?;
    assert_eq!(Some(Ok("Y")), iter.next().map(utf8_to_str));
    assert_eq!(None, iter.next());
    Ok(())
  }

  #[test]
  fn pop_and_move() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let queue_db = QueueDb::open(&env, None)?;
    let queue_1 = queue_db.get("myqueue1");
    let queue_2 = queue_db.get("myqueue2");
    let mut tx = env.begin_rw_txn()?;
    queue_1.push(&mut tx, &[100])?;
    queue_1.push(&mut tx, &[200])?;
    let removed = queue_1.pop_and_move(&mut tx, &queue_2)?;
    assert_eq!(Some(vec![100]), removed);
    tx.commit()?;

    let tx = env.begin_ro_txn()?;
    let mut iter_1 = queue_1.iter(&tx)?;
    assert_eq!(Some(Ok(&[200][..])), iter_1.next());
    assert_eq!(None, iter_1.next());
    let mut iter_2 = queue_2.iter(&tx)?;
    assert_eq!(Some(Ok(&[100][..])), iter_2.next());
    assert_eq!(None, iter_2.next());
    Ok(())
  }

  #[test]
  fn get() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let queue_db = QueueDb::open(&env, None)?;
    let queue_1 = queue_db.get("myqueue1");

    let mut tx = env.begin_rw_txn()?;
    queue_1.push(&mut tx, "zero")?;
    queue_1.push(&mut tx, "one")?;
    queue_1.push(&mut tx, "two")?;
    tx.commit()?;

    let tx = env.begin_ro_txn()?;
    assert_eq!(Ok(Some("zero".as_bytes())), queue_1.get(&tx, 0));
    assert_eq!(Ok(Some("two".as_bytes())), queue_1.get(&tx, 2));
    assert_eq!(Ok(None), queue_1.get(&tx, 3));
    Ok(())
  }

  #[tokio::test]
  async fn pub_sub() -> Result<()> {
    let (_tmpdir, env) = create_env()?;
    let queue_db = QueueDb::open(&env, None)?;
    let queue_1 = queue_db.get("myqueue1");
    let mut subscriber = queue_1.subscribe().await;

    tokio::spawn(async move {
      queue_1.publish(QueueEvent::Push).await;
    });

    assert_eq!(QueueEvent::Push, subscriber.recv().await);
    Ok(())
  }
}
