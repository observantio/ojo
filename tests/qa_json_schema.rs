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

#[test]
fn qa_json_files_have_consistent_top_level_schema() {
    let files = qa_json_files();

    let baseline = read_json(&files[0]);
    let baseline_keys = object_keys(&baseline);

    for path in files.iter().skip(1) {
        let value = read_json(path);
        let keys = object_keys(&value);
        assert_eq!(
            keys,
            baseline_keys,
            "top-level keys differ for {}",
            path.display()
        );
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
        "counter",
        "gauge",
        "gauge_approximation",
        "gauge_derived",
        "gauge_derived_ratio",
        "gauge_ratio",
        "inventory",
        "state",
    ]);
    let expected_namespaces = expected_metric_namespaces();

    for path in qa_json_files() {
        let root = read_json(&path);
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
                metric_name.starts_with("system.") || metric_name.starts_with("process."),
                "{}: metric namespace should start with system. or process.: {}",
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
