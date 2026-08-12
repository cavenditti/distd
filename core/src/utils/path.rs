use std::path::{Component, Path, PathBuf};

use crate::error::{Error, InvalidParameter};

/// Validate an untrusted path as a non-empty relative path.
///
/// Only normal components and `.` are accepted. In particular, absolute paths,
/// platform prefixes and `..` are rejected instead of being normalised.
pub fn validate_relative(path: &Path) -> Result<PathBuf, Error> {
    let mut validated = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => validated.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(InvalidParameter::Generic {
                    expected: "a path contained beneath the configured root".to_string(),
                    got: path.to_string_lossy().into_owned(),
                }
                .into());
            }
        }
    }

    if validated.as_os_str().is_empty() {
        return Err(InvalidParameter::Generic {
            expected: "a non-empty relative path".to_string(),
            got: path.to_string_lossy().into_owned(),
        }
        .into());
    }

    Ok(validated)
}

/// Resolve a relative path, or an already-resolved path below `root`, without
/// permitting lexical escape from `root`.
pub fn resolve_beneath(root: &Path, path: &Path) -> Result<PathBuf, Error> {
    let relative = if path.is_absolute() {
        path.strip_prefix(root).map_err(|_| {
            Error::from(InvalidParameter::Generic {
                expected: format!("an absolute path below {}", root.display()),
                got: path.to_string_lossy().into_owned(),
            })
        })?
    } else {
        path
    };
    Ok(root.join(validate_relative(relative)?))
}

/// Join two paths, removing the eventual leading `/` from the second path
#[must_use]
pub fn join(a: &Path, b: &Path) -> PathBuf {
    a.join(b.strip_prefix("/").unwrap_or(b))
}

#[cfg(test)]
mod tests {
    use super::{resolve_beneath, validate_relative};
    use std::path::Path;

    #[test]
    fn rejects_paths_that_can_escape() {
        for candidate in ["../outside", "nested/../../outside", "/absolute"] {
            assert!(
                validate_relative(Path::new(candidate)).is_err(),
                "{candidate}"
            );
        }
    }

    #[test]
    fn resolves_only_paths_below_root() {
        let root = Path::new("/srv/distd");
        assert_eq!(
            resolve_beneath(root, Path::new("nested/file")).unwrap(),
            root.join("nested/file")
        );
        assert_eq!(
            resolve_beneath(root, Path::new("/srv/distd/nested/file")).unwrap(),
            root.join("nested/file")
        );
        assert!(resolve_beneath(root, Path::new("/srv/other/file")).is_err());
    }
}
