use crate::{
    derive_rates_or_reset, hit_ratio, load_yaml_config_file, parse_bool_env, record_f64,
    record_snapshot, record_u64, resolve_default_config_path, run, saturating_rate,
    update_redis_connection_state, Config, Instruments, PrevState, RedisConfig,
    RedisConnectionState, RedisRates, RedisSnapshot,
};
use host_collectors::{ArchiveStorageConfig, PrefixFilter};
use std::fs;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn unique_temp_path(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!("ojo-redis-{name}-{}-{nanos}", std::process::id()))
}

#[test]
fn parse_bool_env_covers_variants() {
    let _guard = env_lock().lock().expect("env lock");
    std::env::set_var("OJO_REDIS_BOOL", "yes");
    assert_eq!(parse_bool_env("OJO_REDIS_BOOL"), Some(true));
    std::env::set_var("OJO_REDIS_BOOL", "off");
    assert_eq!(parse_bool_env("OJO_REDIS_BOOL"), Some(false));
    std::env::set_var("OJO_REDIS_BOOL", "maybe");
    assert_eq!(parse_bool_env("OJO_REDIS_BOOL"), None);
    std::env::remove_var("OJO_REDIS_BOOL");
}

#[test]
fn rates_helpers_cover_counter_reset_and_ratios() {
    assert_eq!(saturating_rate(10, 5, 1.0), 0.0);
    assert_eq!(saturating_rate(10, 20, 2.0), 5.0);
    assert_eq!(hit_ratio(0, 0), 0.0);
    assert_eq!(hit_ratio(8, 2), 0.8);

    let mut prev = PrevState::default();
    let first = RedisSnapshot {
        available: true,
        commands_processed_total: 100,
        connections_received_total: 10,
        keyspace_hits_total: 8,
        keyspace_misses_total: 2,
        ..RedisSnapshot::default()
    };
    let first_rates = prev.derive(&first);
    assert_eq!(first_rates.commands_per_second, 0.0);
    assert_eq!(first_rates.connections_per_second, 0.0);
    assert_eq!(first_rates.hit_ratio, 0.8);

    prev.last = Some((first, Instant::now() + Duration::from_secs(1)));
    let zero_elapsed = prev.derive(&RedisSnapshot {
        available: true,
        commands_processed_total: 150,
        connections_received_total: 20,
        keyspace_hits_total: 16,
        keyspace_misses_total: 4,
        ..RedisSnapshot::default()
    });
    assert_eq!(zero_elapsed.commands_per_second, 0.0);
    assert_eq!(zero_elapsed.connections_per_second, 0.0);
    assert_eq!(zero_elapsed.hit_ratio, 0.8);

    let reset = derive_rates_or_reset(&mut prev, &RedisSnapshot::default());
    assert_eq!(reset.commands_per_second, 0.0);
    assert_eq!(reset.connections_per_second, 0.0);
    assert_eq!(reset.hit_ratio, 0.0);
    assert!(prev.last.is_none());

    prev.last = Some((
        RedisSnapshot {
            available: true,
            commands_processed_total: 100,
            connections_received_total: 10,
            keyspace_hits_total: 20,
            keyspace_misses_total: 5,
            ..RedisSnapshot::default()
        },
        Instant::now() - Duration::from_secs(1),
    ));
    let available = RedisSnapshot {
        available: true,
        commands_processed_total: 140,
        connections_received_total: 16,
        keyspace_hits_total: 28,
        keyspace_misses_total: 7,
        ..RedisSnapshot::default()
    };
    let rates = derive_rates_or_reset(&mut prev, &available);
    assert!(rates.commands_per_second > 0.0);
    assert!(rates.connections_per_second > 0.0);
    assert!(rates.hit_ratio > 0.0);
}

#[test]
fn redis_connection_state_transitions() {
    assert_eq!(
        update_redis_connection_state(
            RedisConnectionState::Unknown,
            &RedisSnapshot {
                available: true,
                up: true,
                ..RedisSnapshot::default()
            },
        ),
        RedisConnectionState::Connected,
    );

    assert_eq!(
        update_redis_connection_state(RedisConnectionState::Connected, &RedisSnapshot::default(),),
        RedisConnectionState::Disconnected,
    );

    assert_eq!(
        update_redis_connection_state(
            RedisConnectionState::Disconnected,
            &RedisSnapshot {
                available: true,
                up: true,
                ..RedisSnapshot::default()
            },
        ),
        RedisConnectionState::Connected,
    );

    assert_eq!(
        update_redis_connection_state(
            RedisConnectionState::Disconnected,
            &RedisSnapshot::default(),
        ),
        RedisConnectionState::Disconnected,
    );
}

#[test]
fn resolve_default_config_path_prefers_existing_local() {
    let local = unique_temp_path("redis-local.yaml");
    fs::write(&local, "service: {}\n").expect("write local");
    let selected = resolve_default_config_path(local.to_string_lossy().as_ref(), "fallback.yaml");
    assert_eq!(selected, local.to_string_lossy());
    fs::remove_file(&local).expect("cleanup local");

    let selected = resolve_default_config_path("/definitely/missing/redis.yaml", "fallback.yaml");
    assert_eq!(selected, "fallback.yaml");
}

#[test]
fn load_yaml_config_file_covers_missing_empty_invalid_and_valid() {
    let missing = unique_temp_path("redis-missing.yaml");
    let err = load_yaml_config_file(missing.to_string_lossy().as_ref()).unwrap_err();
    assert!(err.to_string().contains("was not found"), "{err}");

    let empty = unique_temp_path("redis-empty.yaml");
    fs::write(&empty, " \n").expect("write empty");
    let err = load_yaml_config_file(empty.to_string_lossy().as_ref()).unwrap_err();
    assert!(err.to_string().contains("is empty"), "{err}");
    fs::remove_file(&empty).expect("cleanup empty");

    let invalid = unique_temp_path("redis-invalid.yaml");
    fs::write(&invalid, "service: [\n").expect("write invalid");
    let err = load_yaml_config_file(invalid.to_string_lossy().as_ref()).unwrap_err();
    assert!(err.to_string().contains("failed to parse YAML"), "{err}");
    fs::remove_file(&invalid).expect("cleanup invalid");

    let valid = unique_temp_path("redis-valid.yaml");
    fs::write(
        &valid,
        "service:\n  name: ojo-redis\ncollection:\n  poll_interval_secs: 1\nredis:\n  executable: redis-cli\n",
    )
    .expect("write valid");
    assert!(load_yaml_config_file(valid.to_string_lossy().as_ref()).is_ok());
    fs::remove_file(&valid).expect("cleanup valid");
}

#[test]
fn load_yaml_config_file_covers_directory_read_error() {
    let dir = unique_temp_path("redis-dir.yaml");
    fs::create_dir_all(&dir).expect("mkdir");
    let err = load_yaml_config_file(dir.to_string_lossy().as_ref()).unwrap_err();
    assert!(
        err.to_string().contains("failed to read config file"),
        "{err}"
    );
    fs::remove_dir_all(&dir).expect("cleanup dir");
}

#[test]
fn config_load_from_args_reads_env_config() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("redis-config.yaml");
    fs::write(
        &path,
        "service:\n  name: ojo-redis-test\ncollection:\n  poll_interval_secs: 1\nredis:\n  executable: redis-cli\n  host: 127.0.0.1\n  port: 6379\n",
    )
    .expect("write config");
    std::env::set_var("OJO_REDIS_CONFIG", &path);

    let args = vec!["ojo-redis".to_string()];
    let cfg = Config::load_from_args(&args).expect("load config");
    assert_eq!(cfg.service_name, "ojo-redis-test");
    assert_eq!(cfg.poll_interval, Duration::from_secs(1));
    assert_eq!(cfg.redis.host.as_deref(), Some("127.0.0.1"));

    std::env::remove_var("OJO_REDIS_CONFIG");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn config_load_from_args_uses_repo_default_when_env_not_set() {
    let _guard = env_lock().lock().expect("env lock");
    std::env::remove_var("OJO_REDIS_CONFIG");
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");

    let args = vec!["ojo-redis".to_string()];
    let cfg = Config::load_from_args(&args).expect("load default config");
    assert!(!cfg.service_name.is_empty());
}

#[test]
fn config_load_from_args_supports_config_flag_and_redis_filters() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("redis-config-flag.yaml");
    fs::write(
        &path,
        "collection:\n  poll_interval_secs: 1\nredis:\n  executable: redis-cli\n  host: 127.0.0.1\n  username: '   '\n  password: secret\n",
    )
    .expect("write config");

    std::env::remove_var("OJO_REDIS_CONFIG");
    let args = vec![
        "ojo-redis".to_string(),
        "--config".to_string(),
        path.to_string_lossy().to_string(),
    ];

    let cfg = Config::load_from_args(&args).expect("load via --config");
    assert_eq!(cfg.service_name, "ojo-redis");
    assert_eq!(cfg.redis.host.as_deref(), Some("127.0.0.1"));
    assert_eq!(cfg.redis.username, None);
    assert_eq!(cfg.redis.password.as_deref(), Some("secret"));

    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn record_u64_covers_allow_and_block_paths() {
    let meter = opentelemetry::global::meter("redis-tests");
    let gauge = meter.u64_gauge("redis.test.value").build();
    let allow = PrefixFilter::new(vec!["system.redis.".to_string()], vec![]);
    record_u64(&gauge, &allow, "system.redis.up", 1);
    let block = PrefixFilter::new(vec!["system.other.".to_string()], vec![]);
    record_u64(&gauge, &block, "system.redis.up", 2);
}

#[test]
fn record_f64_covers_allow_and_block_paths() {
    let meter = opentelemetry::global::meter("redis-tests-f64");
    let gauge = meter.f64_gauge("redis.test.value.f64").build();
    let allow = PrefixFilter::new(vec!["system.redis.".to_string()], vec![]);
    record_f64(
        &gauge,
        &allow,
        "system.redis.commands.processed.rate_per_second",
        1.0,
    );
    let block = PrefixFilter::new(vec!["system.other.".to_string()], vec![]);
    record_f64(
        &gauge,
        &block,
        "system.redis.commands.processed.rate_per_second",
        2.0,
    );
}

#[test]
fn record_snapshot_covers_all_metric_paths() {
    let meter = opentelemetry::global::meter("redis-record-tests");
    let instruments = Instruments::new(&meter);
    let filter = PrefixFilter::new(vec!["system.redis.".to_string()], vec![]);

    record_snapshot(
        &instruments,
        &filter,
        &RedisSnapshot::default(),
        &RedisRates::default(),
    );

    let snapshot = RedisSnapshot {
        available: true,
        up: true,
        connected_clients: 10,
        blocked_clients: 1,
        memory_used_bytes: 1024,
        memory_max_bytes: 2048,
        uptime_seconds: 100,
        commands_processed_total: 300,
        connections_received_total: 50,
        keyspace_hits_total: 70,
        keyspace_misses_total: 30,
        expired_keys_total: 5,
        evicted_keys_total: 2,
    };
    let rates = RedisRates {
        commands_per_second: 20.0,
        connections_per_second: 2.0,
        hit_ratio: 0.7,
    };
    record_snapshot(&instruments, &filter, &snapshot, &rates);
}

#[test]
fn run_once_with_temp_config() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("redis-run.yaml");
    fs::write(
        &path,
        "service:\n  name: ojo-redis-test\ncollection:\n  poll_interval_secs: 1\nredis:\n  executable: redis-cli\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\nstorage:\n  archive_enabled: false\n",
    )
    .expect("write config");

    std::env::set_var("OJO_REDIS_CONFIG", &path);
    std::env::set_var("OJO_RUN_ONCE", "1");
    std::env::set_var("OJO_TEST_MAX_ITERATIONS", "2");
    let fake_info = "connected_clients:10\nblocked_clients:1\nused_memory:1024\nmaxmemory:4096\nuptime_in_seconds:120\ntotal_connections_received:30\ntotal_commands_processed:300\nkeyspace_hits:70\nkeyspace_misses:30\nexpired_keys:5\nevicted_keys:2\n";
    let old = std::env::var("OJO_REDIS_INFO_STUB").ok();
    std::env::set_var("OJO_REDIS_INFO_STUB", fake_info);
    let result = run();
    assert!(result.is_ok(), "{result:?}");

    std::env::remove_var("OJO_REDIS_CONFIG");
    std::env::remove_var("OJO_RUN_ONCE");
    std::env::remove_var("OJO_TEST_MAX_ITERATIONS");
    if let Some(previous) = old {
        std::env::set_var("OJO_REDIS_INFO_STUB", previous);
    } else {
        std::env::remove_var("OJO_REDIS_INFO_STUB");
    }
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn run_supports_test_iteration_cap_when_once_is_false() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("redis-loop.yaml");
    fs::write(
        &path,
        "service:\n  name: ojo-redis-test\ncollection:\n  poll_interval_secs: 1\nredis:\n  executable: redis-cli\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\nstorage:\n  archive_enabled: false\n",
    )
    .expect("write config");

    std::env::set_var("OJO_REDIS_CONFIG", &path);
    std::env::remove_var("OJO_RUN_ONCE");
    std::env::set_var("OJO_TEST_MAX_ITERATIONS", "2");
    let fake_info = "connected_clients:11\nblocked_clients:1\nused_memory:1024\nmaxmemory:4096\nuptime_in_seconds:120\ntotal_connections_received:31\ntotal_commands_processed:301\nkeyspace_hits:71\nkeyspace_misses:31\nexpired_keys:6\nevicted_keys:3\n";
    let old = std::env::var("OJO_REDIS_INFO_STUB").ok();
    std::env::set_var("OJO_REDIS_INFO_STUB", fake_info);

    let result = run();
    assert!(result.is_ok(), "{result:?}");

    std::env::remove_var("OJO_REDIS_CONFIG");
    std::env::remove_var("OJO_TEST_MAX_ITERATIONS");
    if let Some(previous) = old {
        std::env::set_var("OJO_REDIS_INFO_STUB", previous);
    } else {
        std::env::remove_var("OJO_REDIS_INFO_STUB");
    }
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn run_returns_error_for_missing_or_invalid_config() {
    let _guard = env_lock().lock().expect("env lock");

    std::env::set_var("OJO_REDIS_CONFIG", "/definitely/missing/redis.yaml");
    let missing = run();
    assert!(missing.is_err());
    std::env::remove_var("OJO_REDIS_CONFIG");

    let path = unique_temp_path("redis-invalid-protocol.yaml");
    fs::write(
        &path,
        "service:\n  name: ojo-redis-test\ncollection:\n  poll_interval_secs: 1\nredis:\n  executable: redis-cli\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: invalid\n",
    )
    .expect("write config");
    std::env::set_var("OJO_REDIS_CONFIG", &path);
    let invalid = run();
    assert!(invalid.is_err());
    std::env::remove_var("OJO_REDIS_CONFIG");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn config_shape_covers_all_fields() {
    let _cfg_shape = Config {
        service_name: "svc".to_string(),
        instance_id: "inst".to_string(),
        poll_interval: Duration::from_secs(1),
        otlp_endpoint: "http://127.0.0.1:4318/v1/metrics".to_string(),
        otlp_protocol: "http/protobuf".to_string(),
        otlp_headers: std::collections::BTreeMap::new(),
        otlp_compression: None,
        otlp_timeout: None,
        export_interval: None,
        export_timeout: None,
        metrics_include: vec![],
        metrics_exclude: vec![],
        redis: RedisConfig {
            executable: "redis-cli".to_string(),
            host: None,
            port: None,
            username: None,
            password: None,
        },
        archive: ArchiveStorageConfig {
            enabled: false,
            archive_dir: String::new(),
            max_file_bytes: 0,
            retain_files: 0,
            file_stem: "redis-snapshots".to_string(),
        },
        once: true,
    };
}
