use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;

fn qa_json_files() -> Vec<PathBuf> {
    let mut files = fs::read_dir("tests/qa")
        .expect("tests/qa directory must exist")
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect::<Vec<_>>();
    files.sort();
    files
}

fn read_json(path: &Path) -> Value {
    let raw = fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
    serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("failed to parse {} as JSON: {e}", path.display()))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PlatformFixture {
    Linux,
    Windows,
    Solaris,
    Unknown,
}

fn fixture_platform(path: &Path) -> PlatformFixture {
    let name = path
        .file_name()
        .and_then(|v| v.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    if name.contains("windows") {
        return PlatformFixture::Windows;
    }
    if name.contains("solaris") {
        return PlatformFixture::Solaris;
    }
    if [
        "ubuntu",
        "debian",
        "fedora",
        "centos",
        "rocky",
        "almalinux",
        "linux",
    ]
    .iter()
    .any(|token| name.contains(token))
    {
        return PlatformFixture::Linux;
    }
    PlatformFixture::Unknown
}

#[test]
fn qa_metric_classification_platform_prefixes_are_consistent() {
    for path in qa_json_files() {
        let root = read_json(&path);
        let classification = root
            .get("metric_classification")
            .and_then(Value::as_object)
            .expect("metric_classification must be object");
        let platform = fixture_platform(&path);

        for (metric_name, semantic_kind) in classification {
            let semantic_kind = semantic_kind.as_str().unwrap_or("");
            match platform {
                PlatformFixture::Linux => {
                    assert!(
                        !metric_name.starts_with("windows."),
                        "{}: linux fixture includes windows metric '{}'",
                        path.display(),
                        metric_name
                    );
                }
                PlatformFixture::Windows => {
                    let is_linux_prefixed = metric_name.starts_with("system.linux.")
                        || metric_name.starts_with("process.linux.");
                    let is_compat_alias = semantic_kind.starts_with("compatibility_alias");
                    assert!(
                        !is_linux_prefixed || is_compat_alias,
                        "{}: windows fixture includes non-compat linux-prefixed metric '{}' ({})",
                        path.display(),
                        metric_name,
                        semantic_kind
                    );
                }
                PlatformFixture::Solaris => {
                    assert!(
                        !metric_name.starts_with("system.linux.")
                            && !metric_name.starts_with("process.linux.")
                            && !metric_name.starts_with("windows."),
                        "{}: solaris fixture includes os-specific metric '{}'",
                        path.display(),
                        metric_name
                    );
                }
                PlatformFixture::Unknown => {}
            }
        }
    }
}

#[test]
fn qa_system_is_windows_matches_fixture_name() {
    for path in qa_json_files() {
        let root = read_json(&path);
        let is_windows = root
            .get("system")
            .and_then(Value::as_object)
            .and_then(|system| system.get("is_windows"))
            .and_then(Value::as_bool)
            .expect("system.is_windows must be bool");
        let platform = fixture_platform(&path);

        if platform == PlatformFixture::Windows {
            assert!(is_windows, "{} expected windows fixture", path.display());
        }
        if platform == PlatformFixture::Linux || platform == PlatformFixture::Solaris {
            assert!(
                !is_windows,
                "{} expected non-windows fixture",
                path.display()
            );
        }
    }
}
