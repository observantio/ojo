use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::env;
use std::path::Path;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Config {
    pub service_name: String,
    pub instance_id: String,
    pub poll_interval: Duration,
    pub include_process_metrics: bool,
    pub process_include_pid_label: bool,
    pub process_include_command_label: bool,
    pub process_include_state_label: bool,
    pub otlp_endpoint: String,
    pub otlp_protocol: String,
    pub otlp_headers: BTreeMap<String, String>,
    pub otlp_compression: Option<String>,
    pub otlp_timeout: Option<Duration>,
    pub export_interval: Option<Duration>,
    pub export_timeout: Option<Duration>,
    pub metrics_include: Vec<String>,
    pub metrics_exclude: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileConfig {
    service: Option<ServiceSection>,
    collection: Option<CollectionSection>,
    export: Option<ExportSection>,
    metrics: Option<MetricSection>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ServiceSection {
    name: Option<String>,
    instance_id: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct CollectionSection {
    poll_interval_secs: Option<u64>,
    include_process_metrics: Option<bool>,
    process_include_pid_label: Option<bool>,
    process_include_command_label: Option<bool>,
    process_include_state_label: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ExportSection {
    otlp: Option<OtlpSection>,
    batch: Option<BatchSection>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct OtlpSection {
    endpoint: Option<String>,
    protocol: Option<String>,
    token: Option<String>,
    token_header: Option<String>,
    headers: Option<BTreeMap<String, String>>,
    compression: Option<String>,
    timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct BatchSection {
    interval_secs: Option<u64>,
    timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct MetricSection {
    include: Option<Vec<String>>,
    exclude: Option<Vec<String>>,
}

impl Config {
    pub fn load() -> Result<Self> {
        let args = env::args().collect::<Vec<_>>();
        let config_path = args
            .windows(2)
            .find(|pair| pair[0] == "--config")
            .map(|pair| pair[1].clone())
            .or_else(|| env::var("PROC_OTEL_CONFIG").ok())
            .unwrap_or_else(|| "ojo.yaml".to_string());

        let file_cfg = load_yaml_config_file(&config_path)?;
        validate_required_yaml_fields(&file_cfg, &config_path)?;

        let service = file_cfg.service.unwrap_or_default();
        let collection = file_cfg.collection.unwrap_or_default();
        let export = file_cfg.export.unwrap_or_default();
        let otlp = export.otlp.unwrap_or_default();
        let batch = export.batch.unwrap_or_default();
        let metrics = file_cfg.metrics.unwrap_or_default();

        let mut otlp_headers = otlp.headers.unwrap_or_default();
        if let Some(token) = otlp.token {
            let header = otlp
                .token_header
                .unwrap_or_else(|| "authorization".to_string());
            otlp_headers.insert(header, token);
        }

        let otlp_endpoint = otlp
            .endpoint
            .clone()
            .or_else(|| env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok())
            .unwrap_or_else(|| "http://127.0.0.1:4317".to_string());
        let otlp_protocol = otlp
            .protocol
            .or_else(|| env::var("OTEL_EXPORTER_OTLP_PROTOCOL").ok())
            .unwrap_or_else(|| default_protocol_for_endpoint(Some(&otlp_endpoint)));

        Ok(Self {
            service_name: service
                .name
                .or_else(|| env::var("OTEL_SERVICE_NAME").ok())
                .unwrap_or_else(|| "ojo".to_string()),
            instance_id: service
                .instance_id
                .or_else(|| env::var("OTEL_SERVICE_INSTANCE_ID").ok())
                .unwrap_or_else(hostname_fallback),
            poll_interval: Duration::from_secs(
                collection
                    .poll_interval_secs
                    .or_else(|| {
                        env::var("PROC_POLL_INTERVAL_SECS")
                            .ok()
                            .and_then(|v| v.parse().ok())
                    })
                    .unwrap_or(5),
            ),
            include_process_metrics: collection
                .include_process_metrics
                .or_else(|| {
                    env::var("PROC_INCLUDE_PROCESS_METRICS")
                        .ok()
                        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                })
                .unwrap_or(false),
            process_include_pid_label: collection
                .process_include_pid_label
                .or_else(|| parse_bool_env("PROC_PROCESS_INCLUDE_PID_LABEL"))
                .unwrap_or(false),
            process_include_command_label: collection
                .process_include_command_label
                .or_else(|| parse_bool_env("PROC_PROCESS_INCLUDE_COMMAND_LABEL"))
                .unwrap_or(true),
            process_include_state_label: collection
                .process_include_state_label
                .or_else(|| parse_bool_env("PROC_PROCESS_INCLUDE_STATE_LABEL"))
                .unwrap_or(true),
            otlp_endpoint,
            otlp_protocol,
            otlp_headers,
            otlp_compression: otlp
                .compression
                .or_else(|| env::var("OTEL_EXPORTER_OTLP_COMPRESSION").ok()),
            otlp_timeout: otlp.timeout_secs.map(Duration::from_secs).or_else(|| {
                env::var("OTEL_EXPORTER_OTLP_TIMEOUT")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .map(Duration::from_secs)
            }),
            export_interval: batch.interval_secs.map(Duration::from_secs).or_else(|| {
                env::var("OTEL_METRIC_EXPORT_INTERVAL")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .map(Duration::from_millis)
            }),
            export_timeout: batch.timeout_secs.map(Duration::from_secs).or_else(|| {
                env::var("OTEL_METRIC_EXPORT_TIMEOUT")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .map(Duration::from_millis)
            }),
            metrics_include: metrics.include.unwrap_or_default(),
            metrics_exclude: metrics.exclude.unwrap_or_default(),
        })
    }

    pub fn apply_otel_env(&self) {
        env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", &self.otlp_endpoint);
        env::set_var("OTEL_EXPORTER_OTLP_PROTOCOL", &self.otlp_protocol);

        if !self.otlp_headers.is_empty() {
            let headers = self
                .otlp_headers
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join(",");
            env::set_var("OTEL_EXPORTER_OTLP_HEADERS", headers);
        } else {
            env::remove_var("OTEL_EXPORTER_OTLP_HEADERS");
        }

        if let Some(compression) = &self.otlp_compression {
            env::set_var("OTEL_EXPORTER_OTLP_COMPRESSION", compression);
        } else {
            env::remove_var("OTEL_EXPORTER_OTLP_COMPRESSION");
        }
        if let Some(timeout) = self.otlp_timeout {
            env::set_var("OTEL_EXPORTER_OTLP_TIMEOUT", timeout.as_secs().to_string());
        } else {
            env::remove_var("OTEL_EXPORTER_OTLP_TIMEOUT");
        }
        if let Some(interval) = self.export_interval {
            env::set_var(
                "OTEL_METRIC_EXPORT_INTERVAL",
                interval.as_millis().to_string(),
            );
        } else {
            env::remove_var("OTEL_METRIC_EXPORT_INTERVAL");
        }
        if let Some(timeout) = self.export_timeout {
            env::set_var(
                "OTEL_METRIC_EXPORT_TIMEOUT",
                timeout.as_millis().to_string(),
            );
        } else {
            env::remove_var("OTEL_METRIC_EXPORT_TIMEOUT");
        }
    }
}

fn load_yaml_config_file(config_path: &str) -> Result<FileConfig> {
    let path = Path::new(config_path);

    if !path.exists() {
        return Err(anyhow!(
            "config file '{}' was not found. Pass --config <path> or set PROC_OTEL_CONFIG to a valid YAML file.",
            config_path
        ));
    }

    if !path.is_file() {
        return Err(anyhow!(
            "config path '{}' is not a file. Provide a YAML file path.",
            config_path
        ));
    }

    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config file '{}'", config_path))?;

    if contents.trim().is_empty() {
        return Err(anyhow!(
            "config file '{}' is empty. Add required sections like service, collection, and export.otlp.",
            config_path
        ));
    }

    serde_yaml::from_str::<FileConfig>(&contents).with_context(|| {
        format!(
            "failed to parse YAML in '{}'. Check indentation and key/value structure.",
            config_path
        )
    })
}

fn validate_required_yaml_fields(file_cfg: &FileConfig, config_path: &str) -> Result<()> {
    let mut missing = Vec::new();

    let service = file_cfg.service.as_ref();
    let collection = file_cfg.collection.as_ref();
    let export = file_cfg.export.as_ref();
    let otlp = export.and_then(|section| section.otlp.as_ref());

    if service
        .and_then(|section| section.name.as_ref())
        .map(|value| value.trim().is_empty())
        .unwrap_or(true)
    {
        missing.push("service.name");
    }

    if service
        .and_then(|section| section.instance_id.as_ref())
        .map(|value| value.trim().is_empty())
        .unwrap_or(true)
    {
        missing.push("service.instance_id");
    }

    let poll_interval_from_env = env::var("PROC_POLL_INTERVAL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok());
    if collection
        .and_then(|section| section.poll_interval_secs)
        .or(poll_interval_from_env)
        .is_none()
    {
        missing.push("collection.poll_interval_secs");
    }

    if otlp
        .and_then(|section| section.endpoint.as_ref())
        .map(|value| value.trim().is_empty())
        .unwrap_or(true)
    {
        missing.push("export.otlp.endpoint");
    }

    if otlp
        .and_then(|section| section.protocol.as_ref())
        .map(|value| value.trim().is_empty())
        .unwrap_or(true)
    {
        missing.push("export.otlp.protocol");
    }

    if missing.is_empty() {
        return Ok(());
    }

    Err(anyhow!(
        "config file '{}' is missing required attributes:\n- {}\n\nExpected minimal structure:\nservice:\n  name: linux\n  instance_id: linux-0001\ncollection:\n  poll_interval_secs: 5\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf",
        config_path,
        missing.join("\n- ")
    ))
}

fn hostname_fallback() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown-host".to_string())
}

fn parse_bool_env(name: &str) -> Option<bool> {
    let value = env::var(name).ok()?;
    let v = value.trim();
    if v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("yes") {
        return Some(true);
    }
    if v == "0" || v.eq_ignore_ascii_case("false") || v.eq_ignore_ascii_case("no") {
        return Some(false);
    }
    None
}

fn default_protocol_for_endpoint(endpoint: Option<&str>) -> String {
    match endpoint {
        Some(value) if has_non_root_path(value) => "http/protobuf".to_string(),
        _ => "grpc".to_string(),
    }
}

fn has_non_root_path(endpoint: &str) -> bool {
    if let Some((_, rest)) = endpoint.split_once("://") {
        if let Some((_, path)) = rest.split_once('/') {
            return !path.is_empty();
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::{
        default_protocol_for_endpoint, has_non_root_path, hostname_fallback, load_yaml_config_file,
        parse_bool_env, validate_required_yaml_fields, Config, FileConfig,
    };
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::{Mutex, OnceLock};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

        let parsed =
            load_yaml_config_file(valid.to_string_lossy().as_ref()).expect("parse valid yaml");
        let validated = validate_required_yaml_fields(&parsed, valid.to_string_lossy().as_ref());
        assert!(validated.is_ok(), "{validated:?}");

        fs::remove_file(&invalid).expect("cleanup invalid file");
        fs::remove_file(&valid).expect("cleanup valid file");
    }

    #[test]
    fn validate_required_yaml_fields_reports_all_required_attributes() {
        let err =
            validate_required_yaml_fields(&FileConfig::default(), "missing.yaml").unwrap_err();
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
            otlp_endpoint: "http://127.0.0.1:4318/v1/metrics".to_string(),
            otlp_protocol: "http/protobuf".to_string(),
            otlp_headers: headers,
            otlp_compression: Some("gzip".to_string()),
            otlp_timeout: Some(Duration::from_secs(9)),
            export_interval: Some(Duration::from_millis(2500)),
            export_timeout: Some(Duration::from_millis(4500)),
            metrics_include: vec![],
            metrics_exclude: vec![],
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
}
