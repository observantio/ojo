use super::{
    default_protocol_for_endpoint, has_non_root_path, hostname_fallback, load_yaml_config_file,
    parse_bool_env, validate_required_yaml_fields, Config, FileConfig,
};
use host_collectors::ArchiveStorageConfig;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
#[cfg(unix)]
use std::{os::unix::fs::PermissionsExt, path::Path};

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn unique_temp_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!("ojo-{name}-{}-{nanos}", std::process::id()))
}

#[test]
fn detects_non_root_http_path() {
    assert!(has_non_root_path("http://127.0.0.1:4318/v1/metrics"));
    assert!(has_non_root_path("https://collector.example.com/otlp"));
    assert!(!has_non_root_path("http://127.0.0.1:4317"));
    assert!(!has_non_root_path("https://collector.example.com"));
    assert!(!has_non_root_path("collector.example.com/v1/metrics"));
}

#[test]
fn defaults_to_http_protobuf_for_path_endpoints() {
    let protocol = default_protocol_for_endpoint(Some("http://127.0.0.1:4318/v1/metrics"));
    assert_eq!(protocol, "http/protobuf");
}

#[test]
fn defaults_to_grpc_for_root_or_missing_path_endpoints() {
    assert_eq!(
        default_protocol_for_endpoint(Some("http://127.0.0.1:4317")),
        "grpc"
    );
    assert_eq!(default_protocol_for_endpoint(None), "grpc");
}

#[test]
fn load_yaml_config_file_rejects_missing_path() {
    let path = unique_temp_path("missing-config");
    let err = load_yaml_config_file(path.to_string_lossy().as_ref()).unwrap_err();
    assert!(err.to_string().contains("was not found"), "{err}");
}

#[test]
fn load_yaml_config_file_rejects_directory_and_empty_file() {
    let dir_path = unique_temp_path("config-dir");
    fs::create_dir_all(&dir_path).expect("mkdir");
    let dir_err = load_yaml_config_file(dir_path.to_string_lossy().as_ref()).unwrap_err();
    assert!(dir_err.to_string().contains("is not a file"), "{dir_err}");

    let empty_file = unique_temp_path("config-empty.yaml");
    fs::write(&empty_file, " \n  \n").expect("write empty yaml");
    let empty_err = load_yaml_config_file(empty_file.to_string_lossy().as_ref()).unwrap_err();
    assert!(empty_err.to_string().contains("is empty"), "{empty_err}");

    fs::remove_dir_all(&dir_path).expect("cleanup dir");
    fs::remove_file(&empty_file).expect("cleanup empty file");
}

#[test]
fn load_yaml_config_file_rejects_invalid_yaml_and_accepts_valid_yaml() {
    let invalid = unique_temp_path("config-invalid.yaml");
    fs::write(&invalid, "service: [broken").expect("write invalid yaml");
    let invalid_err = load_yaml_config_file(invalid.to_string_lossy().as_ref()).unwrap_err();
    assert!(
        invalid_err.to_string().contains("failed to parse YAML"),
        "{invalid_err}"
    );

    let valid = unique_temp_path("config-valid.yaml");
    fs::write(
            &valid,
            "service:\n  name: linux\n  instance_id: linux-1\ncollection:\n  poll_interval_secs: 5\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n",
        )
        .expect("write valid yaml");

    let parsed = load_yaml_config_file(valid.to_string_lossy().as_ref()).expect("parse valid yaml");
    let validated = validate_required_yaml_fields(&parsed, valid.to_string_lossy().as_ref());
    assert!(validated.is_ok(), "{validated:?}");

    fs::remove_file(&invalid).expect("cleanup invalid file");
    fs::remove_file(&valid).expect("cleanup valid file");
}

#[test]
fn validate_required_yaml_fields_reports_all_required_attributes() {
    let err = validate_required_yaml_fields(&FileConfig::default(), "missing.yaml").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("service.name"), "{msg}");
    assert!(msg.contains("service.instance_id"), "{msg}");
    assert!(msg.contains("export.otlp.endpoint"), "{msg}");
    assert!(msg.contains("export.otlp.protocol"), "{msg}");
}

#[test]
fn parse_bool_env_and_hostname_fallback_cover_common_cases() {
    let _guard = env_lock().lock().expect("env lock");

    std::env::set_var("OJO_BOOL_CASE", " yes ");
    assert_eq!(parse_bool_env("OJO_BOOL_CASE"), Some(true));
    std::env::set_var("OJO_BOOL_CASE", "No");
    assert_eq!(parse_bool_env("OJO_BOOL_CASE"), Some(false));
    std::env::set_var("OJO_BOOL_CASE", "maybe");
    assert_eq!(parse_bool_env("OJO_BOOL_CASE"), None);
    std::env::remove_var("OJO_BOOL_CASE");
    assert_eq!(parse_bool_env("OJO_BOOL_CASE"), None);

    std::env::remove_var("HOSTNAME");
    std::env::remove_var("COMPUTERNAME");
    assert_eq!(hostname_fallback(), "unknown-host");
    std::env::set_var("COMPUTERNAME", "win-host");
    assert_eq!(hostname_fallback(), "win-host");
    std::env::set_var("HOSTNAME", "linux-host");
    assert_eq!(hostname_fallback(), "linux-host");

    std::env::remove_var("HOSTNAME");
    std::env::remove_var("COMPUTERNAME");
}

#[test]
fn apply_otel_env_sets_and_clears_environment_values() {
    let _guard = env_lock().lock().expect("env lock");

    let mut headers = BTreeMap::new();
    headers.insert("authorization".to_string(), "Bearer token".to_string());

    let cfg = Config {
        service_name: "svc".to_string(),
        instance_id: "inst".to_string(),
        poll_interval: Duration::from_secs(5),
        include_process_metrics: true,
        process_include_pid_label: true,
        process_include_command_label: false,
        process_include_state_label: true,
        offline_buffer_intervals: 5,
        otlp_endpoint: "http://127.0.0.1:4318/v1/metrics".to_string(),
        otlp_protocol: "http/protobuf".to_string(),
        otlp_headers: headers,
        otlp_compression: Some("gzip".to_string()),
        otlp_timeout: Some(Duration::from_secs(9)),
        export_interval: Some(Duration::from_millis(2500)),
        export_timeout: Some(Duration::from_millis(4500)),
        metrics_include: vec![],
        metrics_exclude: vec![],
        archive: ArchiveStorageConfig {
            enabled: true,
            archive_dir: "data/ojo".to_string(),
            max_file_bytes: 64 * 1024 * 1024,
            retain_files: 8,
            file_stem: "ojo-snapshots".to_string(),
            format: host_collectors::ArchiveFormat::Parquet,
            mode: host_collectors::ArchiveMode::Trend,
            window_secs: 60,
            compression: host_collectors::ArchiveCompression::Zstd,
        },
    };
    cfg.apply_otel_env();

    assert_eq!(
        std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok().as_deref(),
        Some("http://127.0.0.1:4318/v1/metrics")
    );
    assert_eq!(
        std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL").ok().as_deref(),
        Some("http/protobuf")
    );
    assert_eq!(
        std::env::var("OTEL_EXPORTER_OTLP_COMPRESSION")
            .ok()
            .as_deref(),
        Some("gzip")
    );
    assert_eq!(
        std::env::var("OTEL_EXPORTER_OTLP_TIMEOUT").ok().as_deref(),
        Some("9")
    );
    assert_eq!(
        std::env::var("OTEL_METRIC_EXPORT_INTERVAL").ok().as_deref(),
        Some("2500")
    );
    assert_eq!(
        std::env::var("OTEL_METRIC_EXPORT_TIMEOUT").ok().as_deref(),
        Some("4500")
    );

    let cfg_without_optionals = Config {
        otlp_headers: BTreeMap::new(),
        otlp_compression: None,
        otlp_timeout: None,
        export_interval: None,
        export_timeout: None,
        ..cfg
    };
    cfg_without_optionals.apply_otel_env();

    assert!(std::env::var("OTEL_EXPORTER_OTLP_HEADERS").is_err());
    assert!(std::env::var("OTEL_EXPORTER_OTLP_COMPRESSION").is_err());
    assert!(std::env::var("OTEL_EXPORTER_OTLP_TIMEOUT").is_err());
    assert!(std::env::var("OTEL_METRIC_EXPORT_INTERVAL").is_err());
    assert!(std::env::var("OTEL_METRIC_EXPORT_TIMEOUT").is_err());
}

#[test]
fn config_load_reads_yaml_and_token_header_fields() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("config-load-token.yaml");
    fs::write(
            &path,
            "service:\n  name: core-svc\n  instance_id: core-01\ncollection:\n  poll_interval_secs: 2\n  include_process_metrics: true\n  process_include_pid_label: true\n  process_include_command_label: false\n  process_include_state_label: false\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n    token: abc123\n    token_header: x-api-key\n    compression: gzip\n    timeout_secs: 7\n  batch:\n    interval_secs: 3\n    timeout_secs: 4\nmetrics:\n  include: [system.]\n  exclude: [process.]\n",
        )
        .expect("write config");

    std::env::set_var("PROC_OTEL_CONFIG", &path);
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");

    let cfg = Config::load().expect("load config");
    assert_eq!(cfg.service_name, "core-svc");
    assert_eq!(cfg.instance_id, "core-01");
    assert_eq!(cfg.poll_interval, Duration::from_secs(2));
    assert!(cfg.include_process_metrics);
    assert!(cfg.process_include_pid_label);
    assert!(!cfg.process_include_command_label);
    assert!(!cfg.process_include_state_label);
    assert_eq!(cfg.offline_buffer_intervals, 5);
    assert_eq!(cfg.otlp_endpoint, "http://127.0.0.1:4318/v1/metrics");
    assert_eq!(cfg.otlp_protocol, "http/protobuf");
    assert_eq!(
        cfg.otlp_headers.get("x-api-key").map(String::as_str),
        Some("abc123")
    );
    assert_eq!(cfg.otlp_compression.as_deref(), Some("gzip"));
    assert_eq!(cfg.otlp_timeout, Some(Duration::from_secs(7)));
    assert_eq!(cfg.export_interval, Some(Duration::from_secs(3)));
    assert_eq!(cfg.export_timeout, Some(Duration::from_secs(4)));
    assert_eq!(cfg.metrics_include, vec!["system.".to_string()]);
    assert_eq!(cfg.metrics_exclude, vec!["process.".to_string()]);

    std::env::remove_var("PROC_OTEL_CONFIG");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn config_load_applies_bool_env_parsing() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("config-load-env.yaml");
    fs::write(
            &path,
            "service:\n  name: env-svc\n  instance_id: env-01\ncollection:\n  poll_interval_secs: 1\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n",
        )
        .expect("write config");

    std::env::set_var("PROC_OTEL_CONFIG", &path);
    std::env::set_var("PROC_INCLUDE_PROCESS_METRICS", "true");
    std::env::set_var("PROC_PROCESS_INCLUDE_PID_LABEL", "yes");
    std::env::set_var("PROC_PROCESS_INCLUDE_COMMAND_LABEL", "no");
    std::env::set_var("PROC_PROCESS_INCLUDE_STATE_LABEL", "1");

    let cfg = Config::load().expect("load config");
    assert!(cfg.include_process_metrics);
    assert!(cfg.process_include_pid_label);
    assert!(!cfg.process_include_command_label);
    assert!(cfg.process_include_state_label);
    assert_eq!(cfg.offline_buffer_intervals, 5);

    std::env::remove_var("PROC_OTEL_CONFIG");
    std::env::remove_var("PROC_INCLUDE_PROCESS_METRICS");
    std::env::remove_var("PROC_PROCESS_INCLUDE_PID_LABEL");
    std::env::remove_var("PROC_PROCESS_INCLUDE_COMMAND_LABEL");
    std::env::remove_var("PROC_PROCESS_INCLUDE_STATE_LABEL");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn config_load_accepts_poll_interval_from_env_when_yaml_omits_it() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("config-load-env-poll.yaml");
    fs::write(
            &path,
            "service:\n  name: env-poll\n  instance_id: env-poll-01\ncollection: {}\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n",
        )
        .expect("write config");

    std::env::set_var("PROC_OTEL_CONFIG", &path);
    std::env::set_var("PROC_POLL_INTERVAL_SECS", "7");

    let cfg = Config::load().expect("load config");
    assert_eq!(cfg.poll_interval, Duration::from_secs(7));

    std::env::remove_var("PROC_OTEL_CONFIG");
    std::env::remove_var("PROC_POLL_INTERVAL_SECS");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn config_load_from_args_supports_config_flag() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("config-load-from-args.yaml");
    fs::write(
        &path,
        "service:\n  name: args-svc\n  instance_id: args-01\ncollection:\n  poll_interval_secs: 1\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\n",
    )
    .expect("write config");

    std::env::remove_var("PROC_OTEL_CONFIG");
    let args = vec![
        "ojo".to_string(),
        "--config".to_string(),
        path.to_string_lossy().to_string(),
    ];
    let cfg = Config::load_from_args(&args).expect("load args config");
    assert_eq!(cfg.service_name, "args-svc");

    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn config_load_from_args_uses_default_path_when_no_overrides() {
    let _guard = env_lock().lock().expect("env lock");
    std::env::remove_var("PROC_OTEL_CONFIG");
    let args = vec!["ojo".to_string()];
    let err = Config::load_from_args(&args).unwrap_err();
    assert!(err.to_string().contains("ojo.yaml"), "{err}");
}

#[test]
fn config_load_from_args_surfaces_validation_error() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("config-load-validation.yaml");
    fs::write(&path, "service: {}\ncollection: {}\nexport: {}\n").expect("write config");

    let args = vec![
        "ojo".to_string(),
        "--config".to_string(),
        path.to_string_lossy().to_string(),
    ];
    let err = Config::load_from_args(&args).unwrap_err();
    assert!(
        err.to_string().contains("missing required attributes"),
        "{err}"
    );

    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn from_file_config_covers_env_and_default_fallbacks() {
    let _guard = env_lock().lock().expect("env lock");

    std::env::set_var("OTEL_SERVICE_NAME", "svc-env");
    std::env::set_var("OTEL_SERVICE_INSTANCE_ID", "inst-env");
    std::env::set_var(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "http://127.0.0.1:4318/v1/metrics",
    );
    std::env::set_var("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc");
    std::env::set_var("OTEL_EXPORTER_OTLP_TIMEOUT", "bad");
    std::env::set_var("OTEL_METRIC_EXPORT_INTERVAL", "bad");
    std::env::set_var("OTEL_METRIC_EXPORT_TIMEOUT", "bad");
    std::env::set_var("PROC_INCLUDE_PROCESS_METRICS", "true");
    std::env::set_var("PROC_PROCESS_INCLUDE_PID_LABEL", "1");
    std::env::set_var("PROC_PROCESS_INCLUDE_COMMAND_LABEL", "0");
    std::env::set_var("PROC_PROCESS_INCLUDE_STATE_LABEL", "true");

    let cfg = Config::from_file_config(FileConfig::default());
    assert_eq!(cfg.service_name, "svc-env");
    assert_eq!(cfg.instance_id, "inst-env");
    assert_eq!(cfg.otlp_endpoint, "http://127.0.0.1:4318/v1/metrics");
    assert_eq!(cfg.otlp_protocol, "grpc");
    assert!(cfg.otlp_timeout.is_none());
    assert!(cfg.export_interval.is_none());
    assert!(cfg.export_timeout.is_none());
    assert!(cfg.include_process_metrics);
    assert!(cfg.process_include_pid_label);
    assert!(!cfg.process_include_command_label);
    assert!(cfg.process_include_state_label);

    std::env::set_var("OTEL_EXPORTER_OTLP_TIMEOUT", "9");
    std::env::set_var("OTEL_METRIC_EXPORT_INTERVAL", "2500");
    std::env::set_var("OTEL_METRIC_EXPORT_TIMEOUT", "4500");
    let cfg = Config::from_file_config(FileConfig::default());
    assert_eq!(cfg.otlp_timeout, Some(Duration::from_secs(9)));
    assert_eq!(cfg.export_interval, Some(Duration::from_millis(2500)));
    assert_eq!(cfg.export_timeout, Some(Duration::from_millis(4500)));

    std::env::remove_var("OTEL_SERVICE_NAME");
    std::env::remove_var("OTEL_SERVICE_INSTANCE_ID");
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");
    std::env::remove_var("OTEL_EXPORTER_OTLP_TIMEOUT");
    std::env::remove_var("OTEL_METRIC_EXPORT_INTERVAL");
    std::env::remove_var("OTEL_METRIC_EXPORT_TIMEOUT");
    std::env::remove_var("PROC_INCLUDE_PROCESS_METRICS");
    std::env::remove_var("PROC_PROCESS_INCLUDE_PID_LABEL");
    std::env::remove_var("PROC_PROCESS_INCLUDE_COMMAND_LABEL");
    std::env::remove_var("PROC_PROCESS_INCLUDE_STATE_LABEL");

    let cfg = Config::from_file_config(FileConfig::default());
    assert_eq!(cfg.service_name, "ojo");
    assert_eq!(cfg.otlp_endpoint, "http://127.0.0.1:4317");
    assert_eq!(cfg.otlp_protocol, "grpc");
}

#[test]
fn from_file_config_uses_default_authorization_header_for_token() {
    let cfg = Config::from_file_config(FileConfig {
        export: Some(super::ExportSection {
            otlp: Some(super::OtlpSection {
                token: Some("abc123".to_string()),
                ..super::OtlpSection::default()
            }),
            ..super::ExportSection::default()
        }),
        ..FileConfig::default()
    });
    assert_eq!(
        cfg.otlp_headers.get("authorization").map(String::as_str),
        Some("abc123")
    );
}

#[test]
fn from_file_config_ignores_invalid_offline_buffer_env_value() {
    let _guard = env_lock().lock().expect("env lock");
    std::env::set_var("PROC_OFFLINE_BUFFER_INTERVALS", "not-a-number");

    let cfg = Config::from_file_config(FileConfig::default());
    assert_eq!(
        cfg.offline_buffer_intervals,
        crate::buffers::OFFLINE_BUFFER_INTERVALS
    );

    std::env::remove_var("PROC_OFFLINE_BUFFER_INTERVALS");
}

#[cfg(unix)]
#[test]
fn load_yaml_config_file_reports_read_error_context() {
    let path = unique_temp_path("config-read-error.yaml");
    fs::write(&path, "service:\n  name: x\n").expect("write config");

    let perms = fs::Permissions::from_mode(0o000);
    fs::set_permissions(Path::new(&path), perms).expect("chmod 000");

    let err = load_yaml_config_file(path.to_string_lossy().as_ref()).unwrap_err();
    assert!(
        err.to_string().contains("failed to read config file"),
        "{err}"
    );

    let restore = fs::Permissions::from_mode(0o600);
    fs::set_permissions(Path::new(&path), restore).expect("chmod 600");
    fs::remove_file(&path).expect("cleanup file");
}
