use crate::{
    advance_export_state, bool_as_u64, default_traces_endpoint, derive_trace_line_delta_us,
    find_component_key, handle_flush_event, infer_platform_component, infer_trace_line_component,
    is_reserved_component, load_yaml_config_file, log_flush_result, make_stop_handler,
    normalize_component_stem, parse_bool_env, parse_trace_line_seconds,
    parse_trace_line_seconds_token, record_exporter_state, record_snapshot,
    resolve_default_config_path, ArchivePipeline, ComponentTraceSummary, Config, ExportState,
    FlushEvent, Instruments, SystraceSnapshot,
};
use host_collectors::PrefixFilter;
use opentelemetry::trace::{Span, Tracer};
use std::collections::BTreeMap;
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn unique_temp_path(name: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "ojo-systrace-{name}-{}-{nanos}",
        std::process::id()
    ))
}

#[test]
fn bool_as_u64_maps_cleanly() {
    assert_eq!(bool_as_u64(true), 1);
    assert_eq!(bool_as_u64(false), 0);
}

#[test]
fn parse_bool_env_handles_supported_values() {
    let _guard = env_lock().lock().expect("env lock");
    std::env::set_var("OJO_SYSTRACE_BOOL", "yes");
    assert_eq!(parse_bool_env("OJO_SYSTRACE_BOOL"), Some(true));
    std::env::set_var("OJO_SYSTRACE_BOOL", "off");
    assert_eq!(parse_bool_env("OJO_SYSTRACE_BOOL"), Some(false));
    std::env::set_var("OJO_SYSTRACE_BOOL", "invalid");
    assert_eq!(parse_bool_env("OJO_SYSTRACE_BOOL"), None);
    std::env::remove_var("OJO_SYSTRACE_BOOL");
}

#[test]
fn export_state_and_flush_helpers_cover_all_branches() {
    assert_eq!(
        advance_export_state(ExportState::Pending, true),
        (ExportState::Connected, FlushEvent::Connected)
    );
    assert_eq!(
        advance_export_state(ExportState::Connected, true),
        (ExportState::Connected, FlushEvent::None)
    );
    assert_eq!(
        advance_export_state(ExportState::Reconnecting, true),
        (ExportState::Connected, FlushEvent::Reconnected)
    );
    assert_eq!(
        advance_export_state(ExportState::Connected, false),
        (ExportState::Reconnecting, FlushEvent::Reconnecting)
    );
    assert_eq!(
        advance_export_state(ExportState::Pending, false),
        (ExportState::Reconnecting, FlushEvent::StillUnavailable)
    );

    let started = std::time::Instant::now();
    log_flush_result(started, true);
    log_flush_result(started, false);

    let err = "flush-error";
    handle_flush_event(FlushEvent::Reconnecting, Some(&err));
    handle_flush_event(FlushEvent::StillUnavailable, Some(&err));
    handle_flush_event(FlushEvent::None, Some(&err));
    handle_flush_event(FlushEvent::Connected, Some(&err));
    handle_flush_event(FlushEvent::Reconnected, Some(&err));
    handle_flush_event(FlushEvent::Connected, None);
    handle_flush_event(FlushEvent::Reconnected, None);
    handle_flush_event(FlushEvent::None, None);
}

#[test]
fn default_traces_endpoint_translates_metrics_suffix() {
    assert_eq!(
        default_traces_endpoint("http://127.0.0.1:4318/v1/metrics"),
        "http://127.0.0.1:4318/v1/traces"
    );
    assert_eq!(
        default_traces_endpoint("http://127.0.0.1:4317"),
        "http://127.0.0.1:4317"
    );
}

#[test]
fn resolve_default_config_path_prefers_existing_local() {
    let local = unique_temp_path("systrace-local.yaml");
    fs::write(&local, "service: {}\n").expect("write local");
    let selected = resolve_default_config_path(local.to_string_lossy().as_ref(), "fallback.yaml");
    assert_eq!(selected, local.to_string_lossy());
    fs::remove_file(&local).expect("cleanup local");

    let selected =
        resolve_default_config_path("/definitely/missing/systrace.yaml", "fallback.yaml");
    assert_eq!(selected, "fallback.yaml");
}

#[test]
fn load_yaml_config_file_covers_missing_empty_invalid_and_valid() {
    let missing = unique_temp_path("systrace-missing.yaml");
    let err = load_yaml_config_file(missing.to_string_lossy().as_ref()).unwrap_err();
    assert!(err.to_string().contains("was not found"), "{err}");

    let empty = unique_temp_path("systrace-empty.yaml");
    fs::write(&empty, " \n").expect("write empty");
    let err = load_yaml_config_file(empty.to_string_lossy().as_ref()).unwrap_err();
    assert!(err.to_string().contains("is empty"), "{err}");
    fs::remove_file(&empty).expect("cleanup empty");

    let invalid = unique_temp_path("systrace-invalid.yaml");
    fs::write(&invalid, "service: [\n").expect("write invalid");
    let err = load_yaml_config_file(invalid.to_string_lossy().as_ref()).unwrap_err();
    assert!(err.to_string().contains("failed to parse YAML"), "{err}");
    fs::remove_file(&invalid).expect("cleanup invalid");

    let valid = unique_temp_path("systrace-valid.yaml");
    fs::write(
        &valid,
        "service:\n  name: ojo-systrace\ncollection:\n  poll_interval_secs: 2\n",
    )
    .expect("write valid");
    assert!(load_yaml_config_file(valid.to_string_lossy().as_ref()).is_ok());
    fs::remove_file(&valid).expect("cleanup valid");
}

#[test]
fn config_load_from_args_supports_trace_and_metrics_sections() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("systrace-config.yaml");
    fs::write(
        &path,
        "service:\n  name: ojo-systrace-test\n  instance_id: systrace-test-1\ncollection:\n  poll_interval_secs: 3\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\nmetrics:\n  include: [system.systrace.]\ntraces:\n  enabled: true\n  include: [systrace.]\n",
    )
    .expect("write config");

    std::env::remove_var("OJO_SYSTRACE_CONFIG");
    let args = vec![
        "ojo-systrace".to_string(),
        "--config".to_string(),
        path.to_string_lossy().to_string(),
        "--once".to_string(),
    ];

    let cfg = Config::load_from_args(&args).expect("load config");
    assert_eq!(cfg.service_name, "ojo-systrace-test");
    assert_eq!(cfg.instance_id, "systrace-test-1");
    assert_eq!(cfg.poll_interval, Duration::from_secs(3));
    assert!(cfg.trace_enabled);
    assert_eq!(cfg.metrics_include, vec!["system.systrace."]);
    assert_eq!(cfg.trace_include, vec!["systrace."]);
    assert!(cfg.once);

    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn config_load_from_args_uses_repo_default_when_env_not_set() {
    let _guard = env_lock().lock().expect("env lock");
    std::env::remove_var("OJO_SYSTRACE_CONFIG");
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL");

    let args = vec!["ojo-systrace".to_string()];
    let cfg = Config::load_from_args(&args).expect("load default config");
    assert!(!cfg.service_name.is_empty());
}

#[test]
fn default_snapshot_is_zeroed() {
    let snapshot = SystraceSnapshot::default();
    assert!(!snapshot.available);
    assert_eq!(snapshot.events_total, 0);
    assert_eq!(snapshot.context_switches_per_sec, 0.0);
    assert_eq!(snapshot.interrupts_per_sec, 0.0);
    assert_eq!(snapshot.system_calls_per_sec, 0.0);
    assert_eq!(snapshot.system_calls_source, "");
    assert_eq!(snapshot.system_calls_source_code, 0);
    assert_eq!(snapshot.system_calls_coverage_ratio, 0.0);
    assert_eq!(snapshot.dpcs_per_sec, 0.0);
    assert_eq!(snapshot.process_forks_per_sec, 0.0);
    assert_eq!(snapshot.run_queue_depth, 0.0);
    assert_eq!(snapshot.processes_total, 0);
    assert_eq!(snapshot.threads_total, 0);
    assert_eq!(snapshot.kernel_stack_samples_total, 0);
    assert_eq!(snapshot.user_stack_samples_total, 0);
}

#[test]
fn infer_platform_component_covers_linux_windows_unknown() {
    let linux = SystraceSnapshot {
        tracefs_available: true,
        etw_available: false,
        ..SystraceSnapshot::default()
    };
    assert_eq!(infer_platform_component(&linux), "kernel.linux");

    let windows = SystraceSnapshot {
        tracefs_available: false,
        etw_available: true,
        ..SystraceSnapshot::default()
    };
    assert_eq!(infer_platform_component(&windows), "kernel.windows");

    let unknown = SystraceSnapshot::default();
    assert_eq!(infer_platform_component(&unknown), "kernel.unknown");
}

#[test]
fn infer_trace_line_component_normalizes_kernel_task_names() {
    assert_eq!(
        infer_trace_line_component("<idle>-0     [002] d..2  12.34: func"),
        Some("kernel.idle".to_string())
    );
    assert_eq!(
        infer_trace_line_component("kworker/0:1-123 [000] .... 1.0: foo"),
        Some("kernel.kworker.0".to_string())
    );
    assert_eq!(
        infer_trace_line_component("my-app_worker-42 [001] .... 2.0: bar"),
        Some("kernel.my-app_worker".to_string())
    );
    assert_eq!(
        infer_trace_line_component("=> exc_page_fault"),
        Some("kernel.exc_page_fault".to_string())
    );
    assert_eq!(
        infer_trace_line_component("=>  <000077aab70ac772>"),
        Some("kernel.userstack".to_string())
    );
    assert_eq!(infer_trace_line_component("=>   "), None);
    assert_eq!(
        infer_trace_line_component("worker-abc [001] .... 2.0: bar"),
        Some("kernel.worker-abc".to_string())
    );
    assert_eq!(infer_trace_line_component(""), None);
}

#[test]
fn parse_trace_line_timestamp_yields_boot_relative_time() {
    let line = "<idle>-0     [002] d..2  12.34: func";
    let timestamp = super::parse_trace_line_timestamp(line).expect("should parse timestamp");
    let now = SystemTime::now();
    assert!(
        timestamp <= now,
        "parsed timestamp should not be in the future"
    );
}

#[test]
fn parse_trace_line_timestamp_handles_colons_in_task_name() {
    let line = "kworker/0:1-123 [000] .... 1024.000123: sched_switch: prev=foo next=bar";
    let timestamp = super::parse_trace_line_timestamp(line).expect("should parse timestamp");
    let now = SystemTime::now();
    assert!(
        timestamp <= now,
        "parsed timestamp should not be in the future"
    );
}

#[test]
fn derive_trace_line_delta_us_distributes_sparse_timestamps() {
    let lines = vec![
        "task-1 [000] .... 10.000000: <stack trace>",
        "=> symbol_a",
        "=> symbol_b",
        "task-1 [000] .... 10.001000: sys_enter",
    ];
    let deltas = derive_trace_line_delta_us(&lines);
    assert_eq!(deltas.len(), 4);
    assert!(deltas[0] >= 300);
    assert!(deltas[1] >= 300);
    assert!(deltas[2] >= 300);
}

#[test]
fn derive_trace_line_delta_us_handles_non_positive_deltas() {
    let lines = vec![
        "task-1 [000] .... 10.001000: event-a",
        "task-1 [000] .... 10.000000: event-b",
    ];
    let deltas = derive_trace_line_delta_us(&lines);
    assert_eq!(deltas.len(), 2);
    assert_eq!(deltas[0], 1);
}

#[test]
fn parse_trace_line_helpers_cover_edge_cases() {
    assert_eq!(
        parse_trace_line_seconds_token("cpu 12.5000: event"),
        Some(12.5)
    );
    assert_eq!(parse_trace_line_seconds_token("cpu token:"), None);
    assert_eq!(
        parse_trace_line_seconds("15.25"),
        Some(Duration::new(15, 250_000_000))
    );
    assert_eq!(parse_trace_line_seconds("42"), Some(Duration::new(42, 0)));
    assert_eq!(parse_trace_line_seconds("not-a-number"), None);
    assert_eq!(parse_trace_line_seconds_token("abc 12.x: trace"), None);
    assert_eq!(parse_trace_line_seconds("12.ab"), None);
}

#[test]
fn normalize_component_and_reserved_checks_cover_paths() {
    assert_eq!(
        normalize_component_stem("kworker/0:1"),
        Some("kernel.kworker.0.1".to_string())
    );
    assert_eq!(normalize_component_stem("..///"), None);
    assert_eq!(
        normalize_component_stem("component."),
        Some("kernel.component".to_string())
    );
    assert!(is_reserved_component("kernel.entry_syscall.fast"));
    assert!(!is_reserved_component("kernel.custom_probe"));
}

#[test]
fn find_component_key_prefers_prefix_order() {
    let mut summaries = BTreeMap::new();
    summaries.insert(
        "kernel.do_syscall".to_string(),
        ComponentTraceSummary {
            lines_total: 2,
            delta_us_total: 10,
            sample_line: "line".to_string(),
        },
    );
    summaries.insert(
        "kernel.traceiter".to_string(),
        ComponentTraceSummary {
            lines_total: 1,
            delta_us_total: 5,
            sample_line: "trace".to_string(),
        },
    );

    let found = find_component_key(
        &summaries,
        &["kernel.entry", "kernel.do_sys", "kernel.traceiter"],
    );
    assert_eq!(found, Some("kernel.do_syscall".to_string()));
}

#[test]
fn archive_pipeline_write_snapshot_updates_counters_and_health() {
    let dir = unique_temp_path("systrace-archive-ok");
    fs::create_dir_all(&dir).expect("create dir");
    let mut cfg = Config {
        service_name: "svc".to_string(),
        instance_id: "id".to_string(),
        poll_interval: Duration::from_secs(1),
        otlp_endpoint: "http://127.0.0.1:4318/v1/metrics".to_string(),
        otlp_protocol: "http/protobuf".to_string(),
        otlp_timeout: Some(Duration::from_secs(1)),
        export_interval: Some(Duration::from_secs(1)),
        export_timeout: Some(Duration::from_secs(1)),
        metrics_include: vec!["system.systrace.".to_string()],
        metrics_exclude: vec![],
        trace_enabled: true,
        trace_include: vec!["systrace.".to_string()],
        trace_exclude: vec![],
        archive_enabled: true,
        archive_dir: dir.to_string_lossy().to_string(),
        archive_max_file_bytes: 80,
        archive_retain_files: 2,
        trace_stream_max_lines: 64,
        privileged_expected: true,
        ebpf_enabled: true,
        uprobes_enabled: true,
        usdt_enabled: true,
        runtime_probe_profiles: vec!["default".to_string()],
        once: true,
    };

    let mut archive = ArchivePipeline::from_config(&cfg);
    let snapshot = SystraceSnapshot {
        available: true,
        system_calls_source: "linux_vmstat_nr_syscalls".to_string(),
        processes_total: 10,
        threads_total: 20,
        trace_sample: vec!["line-a".to_string(), "line-b".to_string()],
        ..SystraceSnapshot::default()
    };

    archive.write_snapshot(&snapshot);
    archive.write_snapshot(&snapshot);

    assert!(archive.healthy);
    assert!(archive.last_error.is_none());
    assert!(archive.total_events >= 4);
    assert!(archive.total_bytes > 0);

    cfg.archive_enabled = true;
    let bad_parent = unique_temp_path("systrace-archive-err");
    fs::create_dir_all(&bad_parent).expect("create parent");
    let blocker = bad_parent.join("not-dir");
    fs::write(&blocker, b"file").expect("write blocker");
    cfg.archive_dir = blocker.to_string_lossy().to_string();
    let mut bad_archive = ArchivePipeline::from_config(&cfg);
    bad_archive.write_snapshot(&snapshot);
    assert!(!bad_archive.healthy);
    assert!(bad_archive.last_error.is_some());

    fs::remove_dir_all(&dir).expect("cleanup dir");
    fs::remove_dir_all(&bad_parent).expect("cleanup parent");
}

#[test]
fn archive_pipeline_write_snapshot_handles_empty_trace_sample() {
    let dir = unique_temp_path("systrace-archive-empty-trace");
    fs::create_dir_all(&dir).expect("create dir");
    let cfg = Config {
        service_name: "svc".to_string(),
        instance_id: "id".to_string(),
        poll_interval: Duration::from_secs(1),
        otlp_endpoint: "http://127.0.0.1:4318/v1/metrics".to_string(),
        otlp_protocol: "http/protobuf".to_string(),
        otlp_timeout: Some(Duration::from_secs(1)),
        export_interval: Some(Duration::from_secs(1)),
        export_timeout: Some(Duration::from_secs(1)),
        metrics_include: vec!["system.systrace.".to_string()],
        metrics_exclude: vec![],
        trace_enabled: true,
        trace_include: vec!["systrace.".to_string()],
        trace_exclude: vec![],
        archive_enabled: true,
        archive_dir: dir.to_string_lossy().to_string(),
        archive_max_file_bytes: 1024,
        archive_retain_files: 2,
        trace_stream_max_lines: 16,
        privileged_expected: false,
        ebpf_enabled: false,
        uprobes_enabled: false,
        usdt_enabled: false,
        runtime_probe_profiles: vec![],
        once: true,
    };

    let mut archive = ArchivePipeline::from_config(&cfg);
    let snapshot = SystraceSnapshot {
        available: true,
        system_calls_source: "linux_vmstat_nr_syscalls".to_string(),
        processes_total: 1,
        threads_total: 2,
        trace_sample: vec![],
        ..SystraceSnapshot::default()
    };

    archive.write_snapshot(&snapshot);
    assert!(archive.healthy);
    assert!(archive.total_events >= 1);

    fs::remove_dir_all(&dir).expect("cleanup dir");
}

#[test]
fn archive_pipeline_rotation_and_prune_edges_cover_all_paths() {
    let dir = unique_temp_path("systrace-archive-edges");
    fs::create_dir_all(&dir).expect("create dir");
    let cfg = Config {
        service_name: "svc".to_string(),
        instance_id: "id".to_string(),
        poll_interval: Duration::from_secs(1),
        otlp_endpoint: "http://127.0.0.1:4318/v1/metrics".to_string(),
        otlp_protocol: "http/protobuf".to_string(),
        otlp_timeout: Some(Duration::from_secs(1)),
        export_interval: Some(Duration::from_secs(1)),
        export_timeout: Some(Duration::from_secs(1)),
        metrics_include: vec!["system.systrace.".to_string()],
        metrics_exclude: vec![],
        trace_enabled: false,
        trace_include: vec![],
        trace_exclude: vec![],
        archive_enabled: true,
        archive_dir: dir.to_string_lossy().to_string(),
        archive_max_file_bytes: 16,
        archive_retain_files: 2,
        trace_stream_max_lines: 8,
        privileged_expected: false,
        ebpf_enabled: false,
        uprobes_enabled: false,
        usdt_enabled: false,
        runtime_probe_profiles: vec![],
        once: true,
    };

    let archive = ArchivePipeline::from_config(&cfg);
    let snapshot_path = dir.join("systrace-snapshots.ndjson");
    fs::write(&snapshot_path, "small\n").expect("seed snapshot");
    archive
        .rotate_if_needed(snapshot_path.to_string_lossy().as_ref())
        .expect("small file should not rotate");

    fs::write(
        &snapshot_path,
        "this-line-is-definitely-longer-than-sixteen-bytes\n",
    )
    .expect("seed large snapshot");
    fs::write(format!("{}.1", snapshot_path.to_string_lossy()), "older\n")
        .expect("seed existing rotation");
    archive
        .rotate_if_needed(snapshot_path.to_string_lossy().as_ref())
        .expect("rotate large snapshot");

    fs::write(format!("{}.4", snapshot_path.to_string_lossy()), "stale\n")
        .expect("seed stale rotation");
    archive
        .prune_prefix(snapshot_path.to_string_lossy().as_ref())
        .expect("prune stale rotations");
    assert!(!std::path::Path::new(&format!("{}.4", snapshot_path.to_string_lossy())).exists());

    let mut zero_retain_cfg = cfg.clone();
    zero_retain_cfg.archive_retain_files = 0;
    let zero_retain_archive = ArchivePipeline::from_config(&zero_retain_cfg);
    zero_retain_archive
        .prune_prefix(snapshot_path.to_string_lossy().as_ref())
        .expect("retain zero should be noop");

    fs::remove_dir_all(&dir).expect("cleanup dir");
}

#[test]
fn make_stop_handler_sets_signal_false() {
    let running = Arc::new(AtomicBool::new(true));
    let stop = make_stop_handler(Arc::clone(&running));
    stop();
    assert!(!running.load(Ordering::SeqCst));
}

#[test]
fn emit_trace_snapshot_covers_component_chain_and_probe_spans() {
    let tracer = opentelemetry::global::tracer("systrace-test");
    let mut root_span = tracer.start("systrace.collect");
    let filter = PrefixFilter::new(vec!["systrace.collect".to_string()], vec![]);
    let snapshot = SystraceSnapshot {
        available: true,
        tracefs_available: true,
        etw_available: false,
        tracing_on: true,
        current_tracer: "function_graph".to_string(),
        events_total: 100,
        events_enabled: 20,
        event_categories_total: 10,
        etw_providers_total: 0,
        trace_sample_lines_total: 8,
        context_switches_per_sec: 100.0,
        interrupts_per_sec: 20.0,
        system_calls_per_sec: 2000.0,
        system_calls_source: "linux_vmstat_nr_syscalls".to_string(),
        system_calls_coverage_ratio: 1.0,
        dpcs_per_sec: 1.0,
        process_forks_per_sec: 2.0,
        run_queue_depth: 1.0,
        processes_total: 12,
        threads_total: 34,
        kernel_stack_samples_total: 5,
        user_stack_samples_total: 2,
        enabled_events_inventory_total: 7,
        high_value_categories_targeted: 8,
        high_value_categories_enabled: 6,
        trace_stream_lines_captured_total: 100,
        trace_stream_continuity: true,
        trace_dropped_events_total: 0,
        trace_overrun_events_total: 0,
        syscall_enter_enabled: true,
        syscall_exit_enabled: true,
        privileged_mode: true,
        ebpf_available: true,
        uprobes_available: true,
        usdt_available: true,
        symbolizer_available: true,
        archive_writer_healthy: true,
        archive_events_total: 4,
        archive_bytes_total: 128,
        trace_sample: vec![
            "=> <000077aab70ac772>".to_string(),
            "entry_syscall-1 [000] .... 10.000000: enter".to_string(),
            "do_syscall_64-2 [000] .... 10.000100: call".to_string(),
            "syscall_trace_enter-3 [000] .... 10.000200: trace".to_string(),
            "code-4 [000] .... 10.000300: code".to_string(),
            "__traceiter_sched-5 [000] .... 10.000400: iter".to_string(),
            "custom_probe_a-6 [000] .... 10.000500: p1".to_string(),
            "custom_probe_b-7 [000] .... 10.000600: p2".to_string(),
        ],
        ..SystraceSnapshot::default()
    };

    crate::emit_trace_snapshot(&tracer, &mut root_span, &filter, &snapshot);
    root_span.end();
}

#[test]
fn emit_trace_snapshot_covers_filter_block_and_platform_fallbacks() {
    let tracer = opentelemetry::global::tracer("systrace-test-fallback");
    let mut root_span = tracer.start("systrace.collect");

    let blocked = PrefixFilter::new(vec!["systrace.other".to_string()], vec![]);
    crate::emit_trace_snapshot(
        &tracer,
        &mut root_span,
        &blocked,
        &SystraceSnapshot::default(),
    );

    let allowed = PrefixFilter::new(vec!["systrace.collect".to_string()], vec![]);
    let windows_snapshot = SystraceSnapshot {
        etw_available: true,
        tracefs_available: false,
        ..SystraceSnapshot::default()
    };
    crate::emit_trace_snapshot(&tracer, &mut root_span, &allowed, &windows_snapshot);

    let unavailable_snapshot = SystraceSnapshot::default();
    crate::emit_trace_snapshot(&tracer, &mut root_span, &allowed, &unavailable_snapshot);

    root_span.end();
}

#[test]
fn emit_trace_snapshot_covers_traceiter_parent_and_remaining_components() {
    let tracer = opentelemetry::global::tracer("systrace-test-remaining-components");
    let mut root_span = tracer.start("systrace.collect");
    let filter = PrefixFilter::new(vec!["systrace.collect".to_string()], vec![]);
    let snapshot = SystraceSnapshot {
        trace_sample: vec![
            "__traceiter_sched-1 [000] .... 10.100000: iter".to_string(),
            "return_to_userspace_extra-2 [000] .... 10.200000: ret".to_string(),
        ],
        ..SystraceSnapshot::default()
    };

    crate::emit_trace_snapshot(&tracer, &mut root_span, &filter, &snapshot);
    root_span.end();
}

#[test]
fn emit_trace_snapshot_exercises_traceiter_probe_child_path() {
    let tracer = opentelemetry::global::tracer("systrace-test-traceiter-probe");
    let mut root_span = tracer.start("systrace.collect");
    let filter = PrefixFilter::new(vec!["systrace.collect".to_string()], vec![]);
    let snapshot = SystraceSnapshot {
        trace_sample: vec![
            "__traceiter_sched-1 [000] .... 10.000000: iter".to_string(),
            "custom_probe_xyz-2 [000] .... 10.000100: probe".to_string(),
        ],
        ..SystraceSnapshot::default()
    };

    crate::emit_trace_snapshot(&tracer, &mut root_span, &filter, &snapshot);
    root_span.end();
}

#[test]
fn emit_trace_snapshot_exercises_plain_traceiter_key_path() {
    let tracer = opentelemetry::global::tracer("systrace-test-plain-traceiter");
    let mut root_span = tracer.start("systrace.collect");
    let filter = PrefixFilter::new(vec!["systrace.collect".to_string()], vec![]);
    let snapshot = SystraceSnapshot {
        trace_sample: vec![
            "entry_syscall-1 [000] .... 10.000000: enter".to_string(),
            "traceiter-2 [000] .... 10.000100: iter".to_string(),
            "probe_custom-3 [000] .... 10.000200: probe".to_string(),
        ],
        ..SystraceSnapshot::default()
    };

    crate::emit_trace_snapshot(&tracer, &mut root_span, &filter, &snapshot);
    root_span.end();
}

#[test]
fn record_snapshot_and_exporter_state_cover_metric_paths() {
    let meter = opentelemetry::global::meter("systrace-metrics-test");
    let instruments = Instruments::new(&meter);
    let filter = PrefixFilter::new(vec!["system.systrace.".to_string()], vec![]);
    let snapshot = SystraceSnapshot {
        available: true,
        tracefs_available: true,
        etw_available: true,
        tracing_on: true,
        tracers_available: 3,
        events_total: 4,
        events_enabled: 2,
        buffer_total_kb: 1024,
        etw_sessions_total: 1,
        etw_sessions_running: 1,
        etw_providers_total: 2,
        event_categories_total: 5,
        trace_sample_lines_total: 9,
        context_switches_per_sec: 10.0,
        interrupts_per_sec: 11.0,
        system_calls_per_sec: 12.0,
        system_calls_source_code: 1,
        system_calls_coverage_ratio: 0.9,
        dpcs_per_sec: 13.0,
        process_forks_per_sec: 14.0,
        run_queue_depth: 1.0,
        processes_total: 15,
        threads_total: 16,
        kernel_stack_samples_total: 17,
        user_stack_samples_total: 18,
        collection_errors: 0,
        enabled_events_inventory_total: 19,
        high_value_categories_targeted: 20,
        high_value_categories_enabled: 21,
        trace_stream_lines_captured_total: 22,
        trace_stream_continuity: true,
        trace_dropped_events_total: 0,
        trace_overrun_events_total: 0,
        syscall_enter_enabled: true,
        syscall_exit_enabled: true,
        privileged_mode: true,
        ebpf_available: true,
        uprobes_available: true,
        usdt_available: true,
        symbolizer_available: true,
        archive_writer_healthy: true,
        archive_events_total: 30,
        archive_bytes_total: 31,
        runtime_probes_configured_total: 2,
        ..SystraceSnapshot::default()
    };

    record_snapshot(&instruments, &filter, &snapshot);
    record_exporter_state(&instruments, &filter, ExportState::Connected);
    record_exporter_state(&instruments, &filter, ExportState::Reconnecting);
}

#[test]
fn run_smoke_once_with_temp_config() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("systrace-run-once.yaml");
    fs::write(
        &path,
        "service:\n  name: ojo-systrace-test\ncollection:\n  poll_interval_secs: 1\n  trace_stream_max_lines: 8\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\ntraces:\n  enabled: false\nstorage:\n  archive_enabled: false\ninstrumentation:\n  privileged_expected: false\n  ebpf_enabled: false\n  uprobes_enabled: false\n  usdt_enabled: false\n",
    )
    .expect("write config");

    std::env::set_var("OJO_SYSTRACE_CONFIG", path.to_string_lossy().to_string());
    std::env::set_var("OJO_RUN_ONCE", "1");

    let result = crate::run();
    assert!(
        result.is_ok()
            || result
                .as_ref()
                .err()
                .map(|e| e
                    .to_string()
                    .contains("Ctrl-C signal handler already registered"))
                .unwrap_or(false),
        "run result: {result:?}"
    );

    std::env::remove_var("OJO_SYSTRACE_CONFIG");
    std::env::remove_var("OJO_RUN_ONCE");
    std::env::remove_var("OJO_SYSTRACE_TRACE_STREAM_MAX_LINES");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn run_warns_when_signal_handler_is_already_registered() {
    let _guard = env_lock().lock().expect("env lock");
    let _ = ctrlc::set_handler(|| {});

    let path = unique_temp_path("systrace-run-signal-warn.yaml");
    fs::write(
        &path,
        "service:\n  name: ojo-systrace-signal-test\ncollection:\n  poll_interval_secs: 1\n  trace_stream_max_lines: 8\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\ntraces:\n  enabled: false\nstorage:\n  archive_enabled: false\ninstrumentation:\n  privileged_expected: false\n  ebpf_enabled: false\n  uprobes_enabled: false\n  usdt_enabled: false\n",
    )
    .expect("write config");

    std::env::set_var("OJO_SYSTRACE_CONFIG", path.to_string_lossy().to_string());
    std::env::remove_var("OJO_RUN_ONCE");
    std::env::set_var("OJO_TEST_MAX_ITERATIONS", "1");

    let result = crate::run();
    assert!(result.is_ok(), "run result: {result:?}");

    std::env::remove_var("OJO_SYSTRACE_CONFIG");
    std::env::remove_var("OJO_TEST_MAX_ITERATIONS");
    std::env::remove_var("OJO_SYSTRACE_TRACE_STREAM_MAX_LINES");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn run_warns_when_privileged_expected_but_unprivileged() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("systrace-run-privileged-warn.yaml");
    fs::write(
        &path,
        "service:\n  name: ojo-systrace-priv-test\ncollection:\n  poll_interval_secs: 1\n  trace_stream_max_lines: 8\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\ntraces:\n  enabled: false\nstorage:\n  archive_enabled: false\ninstrumentation:\n  privileged_expected: true\n  ebpf_enabled: false\n  uprobes_enabled: false\n  usdt_enabled: false\n",
    )
    .expect("write config");

    std::env::set_var("OJO_SYSTRACE_CONFIG", path.to_string_lossy().to_string());
    std::env::set_var("OJO_RUN_ONCE", "1");
    std::env::set_var("OJO_SYSTRACE_COVERAGE_PRIVILEGED_MODE", "0");

    let result = crate::run();
    assert!(result.is_ok(), "run result: {result:?}");

    std::env::remove_var("OJO_SYSTRACE_CONFIG");
    std::env::remove_var("OJO_RUN_ONCE");
    std::env::remove_var("OJO_SYSTRACE_COVERAGE_PRIVILEGED_MODE");
    std::env::remove_var("OJO_SYSTRACE_TRACE_STREAM_MAX_LINES");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn run_smoke_once_with_tracing_enabled_stdout_exporter() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("systrace-run-trace-once.yaml");
    fs::write(
        &path,
        "service:\n  name: ojo-systrace-trace-test\ncollection:\n  poll_interval_secs: 1\n  trace_stream_max_lines: 8\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\ntraces:\n  enabled: true\nstorage:\n  archive_enabled: false\ninstrumentation:\n  privileged_expected: false\n  ebpf_enabled: false\n  uprobes_enabled: false\n  usdt_enabled: false\n",
    )
    .expect("write config");

    std::env::set_var("OJO_SYSTRACE_CONFIG", path.to_string_lossy().to_string());
    std::env::set_var("OJO_RUN_ONCE", "1");
    std::env::set_var("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "stdout");
    std::env::set_var(
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "unused-stdout-endpoint",
    );

    let result = crate::run();
    assert!(
        result.is_ok()
            || result
                .as_ref()
                .err()
                .map(|e| e
                    .to_string()
                    .contains("Ctrl-C signal handler already registered"))
                .unwrap_or(false),
        "run result: {result:?}"
    );

    std::env::remove_var("OJO_SYSTRACE_CONFIG");
    std::env::remove_var("OJO_RUN_ONCE");
    std::env::remove_var("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL");
    std::env::remove_var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT");
    std::env::remove_var("OJO_SYSTRACE_TRACE_STREAM_MAX_LINES");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn run_smoke_once_with_tracing_enabled_http_exporter_error_path() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("systrace-run-trace-http-error.yaml");
    fs::write(
        &path,
        "service:\n  name: ojo-systrace-trace-http-error\ncollection:\n  poll_interval_secs: 1\n  trace_stream_max_lines: 8\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\ntraces:\n  enabled: true\nstorage:\n  archive_enabled: false\ninstrumentation:\n  privileged_expected: false\n  ebpf_enabled: false\n  uprobes_enabled: false\n  usdt_enabled: false\n",
    )
    .expect("write config");

    std::env::set_var("OJO_SYSTRACE_CONFIG", path.to_string_lossy().to_string());
    std::env::set_var("OJO_RUN_ONCE", "1");
    std::env::set_var("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/protobuf");
    std::env::set_var(
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "http://127.0.0.1:1/v1/traces",
    );

    let result = crate::run();
    assert!(result.is_ok(), "run result: {result:?}");

    std::env::remove_var("OJO_SYSTRACE_CONFIG");
    std::env::remove_var("OJO_RUN_ONCE");
    std::env::remove_var("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL");
    std::env::remove_var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT");
    std::env::remove_var("OJO_SYSTRACE_TRACE_STREAM_MAX_LINES");
    fs::remove_file(&path).expect("cleanup config");
}

#[test]
fn run_supports_test_iteration_cap_when_once_is_false() {
    let _guard = env_lock().lock().expect("env lock");
    let path = unique_temp_path("systrace-run-loop.yaml");
    fs::write(
        &path,
        "service:\n  name: ojo-systrace-loop-test\ncollection:\n  poll_interval_secs: 1\n  trace_stream_max_lines: 8\nexport:\n  otlp:\n    endpoint: http://127.0.0.1:4318/v1/metrics\n    protocol: http/protobuf\ntraces:\n  enabled: false\nstorage:\n  archive_enabled: false\ninstrumentation:\n  privileged_expected: false\n  ebpf_enabled: false\n  uprobes_enabled: false\n  usdt_enabled: false\n",
    )
    .expect("write config");

    std::env::set_var("OJO_SYSTRACE_CONFIG", path.to_string_lossy().to_string());
    std::env::remove_var("OJO_RUN_ONCE");
    std::env::set_var("OJO_TEST_MAX_ITERATIONS", "2");

    let result = crate::run();
    assert!(result.is_ok(), "run result: {result:?}");

    std::env::remove_var("OJO_SYSTRACE_CONFIG");
    std::env::remove_var("OJO_TEST_MAX_ITERATIONS");
    std::env::remove_var("OJO_SYSTRACE_TRACE_STREAM_MAX_LINES");
    fs::remove_file(&path).expect("cleanup config");
}
