use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;

const EXPECTED_METRIC_NAMESPACES_DEFAULT: &str = "process,system";

fn expected_metric_namespaces() -> BTreeSet<String> {
    let raw = env::var("QA_EXPECTED_METRIC_NAMESPACES")
        .unwrap_or_else(|_| EXPECTED_METRIC_NAMESPACES_DEFAULT.to_string());

    let namespaces = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect::<BTreeSet<_>>();

    assert!(
        !namespaces.is_empty(),
        "QA_EXPECTED_METRIC_NAMESPACES resolved to an empty namespace set"
    );

    namespaces
}

fn qa_json_files() -> Vec<PathBuf> {
    let mut files = fs::read_dir("tests/qa")
        .expect("tests/qa directory must exist")
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect::<Vec<_>>();

    files.sort();
    assert!(
        !files.is_empty(),
        "tests/qa should contain at least one .json file"
    );
    files
}

fn read_json(path: &Path) -> Value {
    let raw = fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
    serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("failed to parse {} as JSON: {e}", path.display()))
}

fn object_keys(value: &Value) -> BTreeSet<String> {
    value
        .as_object()
        .expect("value must be object")
        .keys()
        .cloned()
        .collect()
}

fn is_windows_fixture(root: &Value, path: &Path) -> bool {
    root.get("system")
        .and_then(Value::as_object)
        .and_then(|system| system.get("is_windows"))
        .and_then(Value::as_bool)
        .unwrap_or_else(|| panic!("{}: system.is_windows must be bool", path.display()))
}

fn expected_core_top_level_keys() -> BTreeSet<&'static str> {
    BTreeSet::from([
        "cpuinfo",
        "disks",
        "filesystem",
        "load",
        "memory",
        "metric_classification",
        "mounts",
        "net",
        "net_snmp",
        "processes",
        "sockets",
        "support_state",
        "swaps",
        "system",
        "vmstat",
    ])
}

fn expected_linux_only_top_level_keys() -> BTreeSet<&'static str> {
    BTreeSet::from([
        "buddyinfo",
        "cgroup",
        "net_stat",
        "pressure",
        "pressure_totals_us",
        "runqueue_depth",
        "schedstat",
        "slabinfo",
        "softirqs",
        "softnet",
        "zoneinfo",
    ])
}

#[test]
fn qa_json_files_have_consistent_top_level_schema() {
    let core = expected_core_top_level_keys();
    let linux_only = expected_linux_only_top_level_keys();

    for path in qa_json_files() {
        let root = read_json(&path);
        let keys = object_keys(&root);

        for required in &core {
            assert!(
                keys.contains(*required),
                "{}: missing required top-level key '{}'",
                path.display(),
                required
            );
        }

        if is_windows_fixture(&root, &path) {
            assert!(
                keys.contains("windows"),
                "{}: windows fixture must include top-level 'windows' section",
                path.display()
            );
            for linux_key in &linux_only {
                assert!(
                    !keys.contains(*linux_key),
                    "{}: windows fixture should not include linux-only key '{}'",
                    path.display(),
                    linux_key
                );
            }
        } else {
            assert!(
                !keys.contains("windows"),
                "{}: linux fixture should not include top-level 'windows' section",
                path.display()
            );
            for linux_key in &linux_only {
                assert!(
                    keys.contains(*linux_key),
                    "{}: linux fixture missing linux-only key '{}'",
                    path.display(),
                    linux_key
                );
            }
        }
    }
}

#[test]
fn qa_json_core_sections_have_expected_types() {
    for path in qa_json_files() {
        let root = read_json(&path);

        assert!(
            root.get("system").is_some_and(Value::is_object),
            "{}: missing/invalid system",
            path.display()
        );
        assert!(
            root.get("memory").is_some_and(Value::is_object),
            "{}: missing/invalid memory",
            path.display()
        );
        assert!(
            root.get("load").is_some_and(Value::is_object),
            "{}: missing/invalid load",
            path.display()
        );
        assert!(
            root.get("disks").is_some_and(Value::is_array),
            "{}: missing/invalid disks",
            path.display()
        );
        assert!(
            root.get("net").is_some_and(Value::is_array),
            "{}: missing/invalid net",
            path.display()
        );
        assert!(
            root.get("swaps").is_some_and(Value::is_array),
            "{}: missing/invalid swaps",
            path.display()
        );
        assert!(
            root.get("metric_classification")
                .is_some_and(Value::is_object),
            "{}: missing/invalid metric_classification",
            path.display()
        );
        assert!(
            root.get("support_state").is_some_and(Value::is_object),
            "{}: missing/invalid support_state",
            path.display()
        );

        let system = root
            .get("system")
            .and_then(Value::as_object)
            .expect("system object");
        assert!(
            system.contains_key("is_windows"),
            "{}: system.is_windows missing",
            path.display()
        );
        assert!(
            system.get("is_windows").is_some_and(Value::is_boolean),
            "{}: system.is_windows must be bool",
            path.display()
        );
    }
}

#[test]
fn qa_metric_classification_uses_supported_namespaces_and_semantics() {
    let allowed_semantics = BTreeSet::from([
        "compatibility_alias",
        "compatibility_alias_windows_handle_count",
        "compatibility_alias_windows_priority_class",
        "counter",
        "derived",
        "gauge",
        "gauge_approximation",
        "gauge_derived",
        "gauge_derived_ratio",
        "gauge_ratio",
        "inventory",
        "native",
        "native_windows_analogue",
        "state",
        "synthetic_not_linux_loadavg",
        "unsupported",
        "unsupported_on_windows",
    ]);

    for path in qa_json_files() {
        let root = read_json(&path);
        let expected_namespaces = if is_windows_fixture(&root, &path) {
            BTreeSet::from([
                "process".to_string(),
                "system".to_string(),
                "windows".to_string(),
            ])
        } else {
            expected_metric_namespaces()
        };
        let classification = root
            .get("metric_classification")
            .and_then(Value::as_object)
            .expect("metric_classification must be an object");
        let mut namespaces = BTreeSet::new();

        assert!(
            !classification.is_empty(),
            "{}: metric_classification should not be empty",
            path.display()
        );

        for (metric_name, semantic_kind) in classification {
            let semantic_kind = semantic_kind.as_str().unwrap_or_else(|| {
                panic!(
                    "{}: semantic kind for {} must be string",
                    path.display(),
                    metric_name
                )
            });
            let namespace = metric_name
                .split('.')
                .next()
                .expect("metric name must have at least one token");
            namespaces.insert(namespace.to_string());

            assert!(
                metric_name.starts_with("system.")
                    || metric_name.starts_with("process.")
                    || metric_name.starts_with("windows."),
                "{}: metric namespace should start with system., process., or windows.: {}",
                path.display(),
                metric_name
            );

            assert!(
                allowed_semantics.contains(semantic_kind),
                "{}: unsupported semantic kind '{}' for metric {}",
                path.display(),
                semantic_kind,
                metric_name
            );
        }

        assert_eq!(
            namespaces,
            expected_namespaces,
            "{}: metric namespaces mismatch. expected={:?}, found={:?}",
            path.display(),
            expected_namespaces,
            namespaces
        );
    }
}

#[test]
fn qa_support_state_entries_are_non_empty_strings() {
    for path in qa_json_files() {
        let root = read_json(&path);
        let support_state = root
            .get("support_state")
            .and_then(Value::as_object)
            .expect("support_state must be object");

        assert!(
            !support_state.is_empty(),
            "{}: support_state should not be empty",
            path.display()
        );

        for (k, v) in support_state {
            assert!(
                !k.trim().is_empty(),
                "{}: support_state contains empty key",
                path.display()
            );
            let s = v.as_str().unwrap_or_else(|| {
                panic!(
                    "{}: support_state value for '{}' must be string",
                    path.display(),
                    k
                )
            });
            assert!(
                !s.trim().is_empty(),
                "{}: support_state value for '{}' should not be empty",
                path.display(),
                k
            );
        }
    }
}
