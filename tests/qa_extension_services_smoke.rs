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

    let meter_docker = provider_docker.meter("ojo-docker-smoke");
    let meter_gpu = provider_gpu.meter("ojo-gpu-smoke");
    let meter_sensor = provider_sensor.meter("ojo-sensors-smoke");
    let meter_mysql = provider_mysql.meter("ojo-mysql-smoke");
    let meter_postgres = provider_postgres.meter("ojo-postgres-smoke");
    let meter_nfs = provider_nfs.meter("ojo-nfs-smoke");

    let docker_gauge = meter_docker.f64_gauge("system.docker.smoke.value").build();
    let gpu_gauge = meter_gpu.f64_gauge("system.gpu.smoke.value").build();
    let sensor_gauge = meter_sensor.f64_gauge("system.sensor.smoke.value").build();
    let mysql_gauge = meter_mysql.f64_gauge("system.mysql.smoke.value").build();
    let postgres_gauge = meter_postgres
        .f64_gauge("system.postgres.smoke.value")
        .build();
    let nfs_gauge = meter_nfs.f64_gauge("system.nfs_client.smoke.value").build();

    docker_gauge.record(1.0, &[]);
    gpu_gauge.record(1.0, &[]);
    sensor_gauge.record(1.0, &[]);
    mysql_gauge.record(1.0, &[]);
    postgres_gauge.record(1.0, &[]);
    nfs_gauge.record(1.0, &[]);

    let _ = provider_docker.force_flush();
    let _ = provider_gpu.force_flush();
    let _ = provider_sensor.force_flush();
    let _ = provider_mysql.force_flush();
    let _ = provider_postgres.force_flush();
    let _ = provider_nfs.force_flush();

    let _ = provider_docker.shutdown();
    let _ = provider_gpu.shutdown();
    let _ = provider_sensor.shutdown();
    let _ = provider_mysql.shutdown();
    let _ = provider_postgres.shutdown();
    let _ = provider_nfs.shutdown();
    Ok(())
}
