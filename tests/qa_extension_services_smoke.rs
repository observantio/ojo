use anyhow::Result;
use host_collectors::{build_meter_provider, OtlpSettings};
use opentelemetry::metrics::MeterProvider;
use std::collections::BTreeMap;

fn settings(service_name: &str, instance_id: &str) -> OtlpSettings {
    OtlpSettings {
        service_name: service_name.to_string(),
        instance_id: instance_id.to_string(),
        otlp_endpoint: "http://127.0.0.1:4318/v1/metrics".to_string(),
        otlp_protocol: "http/protobuf".to_string(),
        otlp_headers: BTreeMap::new(),
        otlp_compression: None,
        otlp_timeout: None,
        export_interval: None,
        export_timeout: None,
    }
}

#[test]
fn extension_services_can_share_single_otlp_endpoint() -> Result<()> {
    let provider_docker = build_meter_provider(&settings("ojo-docker", "docker-smoke"))?;
    let provider_gpu = build_meter_provider(&settings("ojo-gpu", "gpu-smoke"))?;
    let provider_sensor = build_meter_provider(&settings("ojo-sensors", "sensor-smoke"))?;
    let provider_mysql = build_meter_provider(&settings("ojo-mysql", "mysql-smoke"))?;
    let provider_postgres = build_meter_provider(&settings("ojo-postgres", "postgres-smoke"))?;
    let provider_nfs = build_meter_provider(&settings("ojo-nfs-client", "nfs-smoke"))?;
    let provider_nginx = build_meter_provider(&settings("ojo-nginx", "nginx-smoke"))?;
    let provider_redis = build_meter_provider(&settings("ojo-redis", "redis-smoke"))?;
    let provider_systemd = build_meter_provider(&settings("ojo-systemd", "systemd-smoke"))?;
    let provider_systrace = build_meter_provider(&settings("ojo-systrace", "systrace-smoke"))?;
    let provider_syslog = build_meter_provider(&settings("ojo-syslog", "syslog-smoke"))?;

    let meter_docker = provider_docker.meter("ojo-docker-smoke");
    let meter_gpu = provider_gpu.meter("ojo-gpu-smoke");
    let meter_sensor = provider_sensor.meter("ojo-sensors-smoke");
    let meter_mysql = provider_mysql.meter("ojo-mysql-smoke");
    let meter_postgres = provider_postgres.meter("ojo-postgres-smoke");
    let meter_nfs = provider_nfs.meter("ojo-nfs-smoke");
    let meter_nginx = provider_nginx.meter("ojo-nginx-smoke");
    let meter_redis = provider_redis.meter("ojo-redis-smoke");
    let meter_systemd = provider_systemd.meter("ojo-systemd-smoke");
    let meter_systrace = provider_systrace.meter("ojo-systrace-smoke");
    let meter_syslog = provider_syslog.meter("ojo-syslog-smoke");

    let docker_gauge = meter_docker.f64_gauge("system.docker.smoke.value").build();
    let gpu_gauge = meter_gpu.f64_gauge("system.gpu.smoke.value").build();
    let sensor_gauge = meter_sensor.f64_gauge("system.sensor.smoke.value").build();
    let mysql_gauge = meter_mysql.f64_gauge("system.mysql.smoke.value").build();
    let postgres_gauge = meter_postgres
        .f64_gauge("system.postgres.smoke.value")
        .build();
    let nfs_gauge = meter_nfs.f64_gauge("system.nfs_client.smoke.value").build();
    let nginx_gauge = meter_nginx.f64_gauge("system.nginx.smoke.value").build();
    let redis_gauge = meter_redis.f64_gauge("system.redis.smoke.value").build();
    let systemd_gauge = meter_systemd
        .f64_gauge("system.systemd.smoke.value")
        .build();
    let systrace_gauge = meter_systrace
        .f64_gauge("system.systrace.smoke.value")
        .build();
    let syslog_gauge = meter_syslog.f64_gauge("system.syslog.smoke.value").build();

    docker_gauge.record(1.0, &[]);
    gpu_gauge.record(1.0, &[]);
    sensor_gauge.record(1.0, &[]);
    mysql_gauge.record(1.0, &[]);
    postgres_gauge.record(1.0, &[]);
    nfs_gauge.record(1.0, &[]);
    nginx_gauge.record(1.0, &[]);
    redis_gauge.record(1.0, &[]);
    systemd_gauge.record(1.0, &[]);
    systrace_gauge.record(1.0, &[]);
    syslog_gauge.record(1.0, &[]);

    let _ = provider_docker.force_flush();
    let _ = provider_gpu.force_flush();
    let _ = provider_sensor.force_flush();
    let _ = provider_mysql.force_flush();
    let _ = provider_postgres.force_flush();
    let _ = provider_nfs.force_flush();
    let _ = provider_nginx.force_flush();
    let _ = provider_redis.force_flush();
    let _ = provider_systemd.force_flush();
    let _ = provider_systrace.force_flush();
    let _ = provider_syslog.force_flush();

    let _ = provider_docker.shutdown();
    let _ = provider_gpu.shutdown();
    let _ = provider_sensor.shutdown();
    let _ = provider_mysql.shutdown();
    let _ = provider_postgres.shutdown();
    let _ = provider_nfs.shutdown();
    let _ = provider_nginx.shutdown();
    let _ = provider_redis.shutdown();
    let _ = provider_systemd.shutdown();
    let _ = provider_systrace.shutdown();
    let _ = provider_syslog.shutdown();
    Ok(())
}
