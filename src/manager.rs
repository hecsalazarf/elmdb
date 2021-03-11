use crate::environment::Env;
use lmdb::{EnvironmentBuilder as Builder, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Once, RwLock};

/// A singleton that controls access to environments.
///
/// This manager enforces that only one `Env` at specfic `Path`
/// exists in a process.
#[derive(Debug)]
pub struct Manager {
  envs: HashMap<PathBuf, Env>,
}

impl Manager {
  /// Returns the 'Manager' singleton.
  pub fn singleton() -> &'static RwLock<Manager> {
    static START: Once = Once::new();
    static mut MANAGER: Option<RwLock<Manager>> = None;

    // Safe because we only mutate once in a synchronized fashion
    unsafe {
      START.call_once(|| {
        let manager = RwLock::new(Manager {
          envs: HashMap::new(),
        });
        MANAGER = Some(manager);
      });
      MANAGER.as_ref().unwrap()
    }
  }

  /// Gets an existant `Env` at specified `path` or creates one from the `builder`.
  pub fn get_or_init<P>(&mut self, builder: Builder, path: P) -> Result<Env>
  where
    P: AsRef<Path>,
  {
    let path = path.as_ref();
    if let Some(env) = self.envs.get(path) {
      return Ok(env.clone());
    }

    let env = Env::open(builder.open(path)?)?;
    self.envs.insert(path.into(), env.clone());

    Ok(env)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::test_utils::default_env_builder;

  #[test]
  fn get_or_init() -> Result<()> {
    let singleton = Manager::singleton();
    let tmp_dir = tempfile::Builder::new()
      .prefix("lmdb")
      .tempdir()
      .expect("tmp dir");

    for _ in 0..3 {
      // Call get_or_init 3 times
      let mut manager = singleton.write().unwrap();
      manager.get_or_init(default_env_builder(), tmp_dir.path())?;
    }
    let manager = singleton.read().unwrap();
    // Only one environment created
    assert_eq!(1, manager.envs.len());
    Ok(())
  }
}
