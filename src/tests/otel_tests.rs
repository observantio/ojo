use super::init_meter_provider;
use crate::config::{Config, HostType, MetricCardinalityConfig, TieredReplayConfig};
use host_collectors::ArchiveStorageConfig;
use std::collections::BTreeMap;
use std::time::Duration;

fn test_config(protocol: &str, endpoint: &str) -> Config {
    Config {
        service_name: "test-service".to_string(),
        instance_id: "test-instance".to_string(),
        host_type: HostType::Auto,
        poll_interval: Duration::from_secs(1),
        include_process_metrics: false,
        process_include_pid_label: false,
        process_include_command_label: true,
        process_include_state_label: true,
        offline_buffer_intervals: 5,
        otlp_endpoint: endpoint.to_string(),
        otlp_protocol: protocol.to_string(),
        otlp_headers: BTreeMap::new(),
        otlp_compression: None,
        otlp_timeout: Some(Duration::from_secs(1)),
        export_interval: None,
        export_timeout: None,
        metrics_include: vec![],
        metrics_exclude: vec![],
        metric_cardinality: MetricCardinalityConfig {
            process_max_series: 20000,
            cgroup_max_series: 10000,
        },
        archive: ArchiveStorageConfig {
            enabled: false,
            archive_dir: String::new(),
            max_file_bytes: 0,
            retain_files: 0,
            file_stem: "ojo-snapshots".to_string(),
            format: host_collectors::ArchiveFormat::Parquet,
            mode: host_collectors::ArchiveMode::Trend,
            window_secs: 60,
            compression: host_collectors::ArchiveCompression::Zstd,
        },
        tiered_replay: TieredReplayConfig {
            enabled: false,
            memory_cap_items: 32,
            wal_dir: "data/ojo/tiered-replay".to_string(),
            wal_segment_max_bytes: 1024 * 1024,
            wal_segment_max_age_secs: 60,
            max_replay_per_tick: 32,
        },
    }
}

#[test]
fn init_meter_provider_rejects_unsupported_protocol() {
    let cfg = test_config("invalid", "http://127.0.0.1:4317");
    let err = init_meter_provider(&cfg).unwrap_err();
    assert!(
        err.to_string().contains("unsupported OTLP protocol"),
        "{err}"
    );
}

#[test]
fn init_meter_provider_accepts_http_protobuf() {
    let cfg = test_config("http/protobuf", "http://127.0.0.1:4318/v1/metrics");
    let provider = init_meter_provider(&cfg);
    assert!(provider.is_ok(), "{provider:?}");
}
