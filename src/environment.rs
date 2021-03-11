use crate::transaction::{RwTxn, TxnStorage};
use lmdb::{Environment, Result};
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::Semaphore;

/// A thread-safe LMDB environment protected by an `Arc`.
///
/// An environment supports multiple databases, all residing in the same
/// shared-memory map. It can be cloned and sent across threads; providing an async
/// version to begin a read-write transaction.
#[derive(Debug, Clone)]
pub struct Env {
  inner: Arc<EnvInner>,
}

impl Env {
  pub(crate) fn open(environment: Environment) -> Result<Self> {
    let txn_storage = TxnStorage::open(&environment)?;
    Ok(Self {
      inner: Arc::new(EnvInner::new(environment, txn_storage)),
    })
  }

  /// Asynchronously creates a read-write transaction for use with the environment. This method
  /// will yield while there are any other read-write transaction open on the environment.
  ///
  /// # Note
  /// Only for single process access. Multi-process access will still block the thread if there
  /// is an active read-write transaction.
  pub async fn begin_rw_txn_async<'env>(&'env self) -> Result<RwTxn<'env>> {
    self.inner.begin_rw_txn(None).await
  }

  /// Creates an idempotent read-write transaction, which can be applied multiple times without
  /// changing the result beyond the initial application. If subsequent transaction are begun
  /// with the same `id`, the transaction will be aborted on commit.
  ///
  /// To check if the transaction was previously committed, use `RwTxn::recover`.
  pub async fn begin_idemp_txn<'env>(&'env self, id: u128) -> Result<RwTxn<'env>> {
    self.inner.begin_rw_txn(Some(id)).await
  }
}

impl Deref for Env {
  type Target = Environment;

  fn deref(&self) -> &Self::Target {
    &self.inner.environment
  }
}

#[derive(Debug)]
struct EnvInner {
  environment: Environment,
  semaphore: Semaphore,
  txn_store: TxnStorage,
}

impl EnvInner {
  fn new(environment: Environment, txn_store: TxnStorage) -> Self {
    Self {
      environment,
      semaphore: Semaphore::new(1),
      txn_store,
    }
  }

  async fn begin_rw_txn<'env>(&'env self, id: Option<u128>) -> Result<RwTxn<'env>> {
    let permit = self.semaphore.acquire().await.expect("acquire permit");
    let txn = self.environment.begin_rw_txn()?;
    let store = if id.is_some() {
      Some(&self.txn_store)
    } else {
      None
    };

    Ok(RwTxn::new(permit, txn, id, store))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::test_utils::create_env;
  use lmdb::Transaction;
  use lmdb::{DatabaseFlags, WriteFlags};

  #[tokio::test]
  async fn no_tls() -> Result<()> {
    use std::time::Duration;
    // THIS TEST MUST FAIL WHEN NO_TLS IS DISABLED FOR THE ENVIRONMENT
    // See https://github.com/hecsalazarf/bjsys/issues/52 for more info.

    // create_env() automatically inserts NO_TLS
    let (_tmpdir, raw_env) = create_env()?;
    let env = Env::open(raw_env)?;

    let db = env.create_db(Some("hello"), DatabaseFlags::default())?;
    let mut txw = env.begin_rw_txn_async().await?;
    txw.put(db, &"hello", b"world", WriteFlags::default())?;
    txw.commit()?;

    let env2 = env.clone();
    let handler = tokio::spawn(async move {
      tokio::time::sleep(Duration::from_millis(100)).await;
      let tx2 = env2.begin_ro_txn().expect("ro txn");
      assert_eq!(Ok(&b"world"[..]), tx2.get(db, &"hello"));
      tokio::time::sleep(Duration::from_millis(300)).await;
    });

    let mut txw = env.begin_rw_txn_async().await?;
    txw.put(db, &"hello", b"friend", WriteFlags::default())?;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    txw.commit()?;
    // If NO_TLS disabled, here the transaction will fail since the spawned tokio
    // task runs on the same OS thread, and such task still locks the table slot.
    let tx1 = env.begin_ro_txn()?;
    assert_eq!(Ok(&b"friend"[..]), tx1.get(db, &"hello"));
    handler.await.unwrap();
    Ok(())
  }
}
