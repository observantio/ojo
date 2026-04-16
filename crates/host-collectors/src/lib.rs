use anyhow::Result;
use opentelemetry::global;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{Protocol, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{
    error::{OTelSdkError, OTelSdkResult},
    metrics::{PeriodicReader, SdkMeterProvider},
    resource::Resource,
    trace::{BatchConfigBuilder, BatchSpanProcessor, SdkTracerProvider, SpanData, SpanExporter},
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

pub const METRIC_PREFIX_SYSTEM: &str = "system.";

#[derive(Clone, Debug)]
pub struct ArchiveStorageConfig {
    pub enabled: bool,
    pub archive_dir: String,
    pub max_file_bytes: u64,
    pub retain_files: usize,
    pub file_stem: String,
}

impl ArchiveStorageConfig {
    pub fn disabled(default_stem: &str) -> Self {
        Self {
            enabled: false,
            archive_dir: String::new(),
            max_file_bytes: 0,
            retain_files: 0,
            file_stem: default_stem.to_string(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct JsonArchiveWriter {
    enabled: bool,
    dir: String,
    max_file_bytes: u64,
    retain_files: usize,
    file_stem: String,
    pub total_records: u64,
    pub total_bytes: u64,
    pub healthy: bool,
    pub last_error: Option<String>,
}

impl JsonArchiveWriter {
    pub fn from_config(config: &ArchiveStorageConfig) -> Self {
        Self {
            enabled: config.enabled,
            dir: config.archive_dir.clone(),
            max_file_bytes: config.max_file_bytes,
            retain_files: config.retain_files,
            file_stem: config.file_stem.clone(),
            total_records: 0,
            total_bytes: 0,
            healthy: true,
            last_error: None,
        }
    }

    pub fn write_json_line(&mut self, value: &Value) {
        if !self.enabled {
            return;
        }
        if let Err(err) = self.write_json_line_impl(value) {
            self.healthy = false;
            self.last_error = Some(err.to_string());
        } else {
            self.healthy = true;
            self.last_error = None;
        }
    }

    fn write_json_line_impl(&mut self, value: &Value) -> anyhow::Result<()> {
        fs::create_dir_all(&self.dir)?;
        let path = format!("{}/{}.ndjson", self.dir, self.file_stem);
        self.rotate_if_needed(&path)?;

        let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
        let line = value.to_string();
        file.write_all(line.as_bytes())?;
        file.write_all(b"\n")?;
        self.total_records = self.total_records.saturating_add(1);
        self.total_bytes = self
            .total_bytes
            .saturating_add((line.len() as u64).saturating_add(1));

        self.prune_rotated_files(&path)?;
        Ok(())
    }

    fn rotate_if_needed(&self, path: &str) -> anyhow::Result<()> {
        let metadata = match fs::metadata(path) {
            Ok(md) => md,
            Err(_) => return Ok(()),
        };
        if metadata.len() < self.max_file_bytes {
            return Ok(());
        }
        for idx in (1..=self.retain_files).rev() {
            let src = format!("{}.{}", path, idx);
            let dst = format!("{}.{}", path, idx + 1);
            if Path::new(&src).exists() {
                let _ = fs::rename(&src, &dst);
            }
        }
        let _ = fs::rename(path, format!("{}.1", path));
        Ok(())
    }

    fn prune_rotated_files(&self, prefix: &str) -> anyhow::Result<()> {
        if self.retain_files == 0 {
            return Ok(());
        }
        let mut idx = self.retain_files + 2;
        loop {
            let candidate = format!("{}.{}", prefix, idx);
            if !Path::new(&candidate).exists() {
                break;
            }
            fs::remove_file(&candidate)?;
            idx += 1;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct PrefixFilter {
    include: Vec<String>,
    exclude: Vec<String>,
}

impl PrefixFilter {
    pub fn new(include: Vec<String>, exclude: Vec<String>) -> Self {
        Self { include, exclude }
    }

    #[must_use]
    pub fn allows(&self, name: &str) -> bool {
        let include_match =
            self.include.is_empty() || self.include.iter().any(|p| name.starts_with(p));
        let exclude_match = self.exclude.iter().any(|p| name.starts_with(p));
        include_match && !exclude_match
    }
}

#[derive(Clone, Debug)]
pub struct OtlpSettings {
    pub service_name: String,
    pub instance_id: String,
    pub otlp_endpoint: String,
    pub otlp_protocol: String,
    pub otlp_headers: BTreeMap<String, String>,
    pub otlp_compression: Option<String>,
    pub otlp_timeout: Option<Duration>,
    pub export_interval: Option<Duration>,
    pub export_timeout: Option<Duration>,
}

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

pub fn build_meter_provider(settings: &OtlpSettings) -> Result<SdkMeterProvider> {
    let timeout = settings.otlp_timeout.unwrap_or(DEFAULT_TIMEOUT);

    let exporter = match settings.otlp_protocol.as_str() {
        "http/protobuf" => {
            let mut builder = opentelemetry_otlp::MetricExporter::builder()
                .with_http()
                .with_protocol(Protocol::HttpBinary)
                .with_endpoint(settings.otlp_endpoint.clone())
                .with_timeout(timeout);
            #[cfg(not(target_os = "solaris"))]
            {
                builder = builder.with_http_client(reqwest::blocking::Client::new());
            }
            builder.build()?
        }
        "grpc" => opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_endpoint(settings.otlp_endpoint.clone())
            .with_timeout(timeout)
            .build()?,
        other => {
            anyhow::bail!(
                "unsupported OTLP protocol: {other:?}; expected \"http/protobuf\" or \"grpc\""
            );
        }
    };

    let mut reader_builder = PeriodicReader::builder(exporter);
    if let Some(interval) = settings.export_interval {
        reader_builder = reader_builder.with_interval(interval);
    }
    let reader = reader_builder.build();

    let provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(build_resource(settings))
        .build();

    Ok(provider)
}

pub fn init_meter_provider(settings: &OtlpSettings) -> Result<SdkMeterProvider> {
    let provider = build_meter_provider(settings)?;
    global::set_meter_provider(provider.clone());
    Ok(provider)
}

#[derive(Debug)]
struct StdoutSpanExporter {
    resource: Resource,
    is_shutdown: AtomicBool,
}

impl Default for StdoutSpanExporter {
    fn default() -> Self {
        StdoutSpanExporter {
            resource: Resource::builder().build(),
            is_shutdown: AtomicBool::new(false),
        }
    }
}

impl SpanExporter for StdoutSpanExporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        if self.is_shutdown.load(Ordering::SeqCst) {
            return Err(OTelSdkError::AlreadyShutdown);
        }

        println!("Spans");
        if let Some(schema_url) = self.resource.schema_url() {
            println!("\tResource SchemaUrl: {schema_url:?}");
        }
        self.resource.iter().for_each(|(k, v)| {
            println!("\t ->  {k}={v:?}");
        });

        for (i, span) in batch.into_iter().enumerate() {
            println!("Span #{i}");
            println!("\tName         : {}", span.name);
            println!("\tTraceId      : {}", span.span_context.trace_id());
            println!("\tSpanId       : {}", span.span_context.span_id());
            println!("\tTraceFlags   : {:?}", span.span_context.trace_flags());
            if span.parent_span_id == opentelemetry::SpanId::INVALID {
                println!("\tParentSpanId : None (root span)");
            } else {
                println!("\tParentSpanId : {}", span.parent_span_id);
            }
            println!("\tKind         : {:?}", span.span_kind);
            println!("\tStatus       : {:?}", span.status);
            if !span.attributes.is_empty() {
                println!("\tAttributes:");
                span.attributes.iter().for_each(|kv| {
                    println!("\t\t ->  {}: {:?}", kv.key, kv.value);
                });
            }
        }

        Ok(())
    }

    fn shutdown(&mut self) -> OTelSdkResult {
        self.is_shutdown.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.resource = resource.clone();
    }
}

pub fn build_tracer_provider(settings: &OtlpSettings) -> Result<SdkTracerProvider> {
    if settings.otlp_protocol == "stdout" {
        let exporter = StdoutSpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter)
            .with_resource(build_resource(settings))
            .build();
        return Ok(provider);
    }

    let timeout = settings.otlp_timeout.unwrap_or(DEFAULT_TIMEOUT);

    let exporter = match settings.otlp_protocol.as_str() {
        "http/protobuf" => {
            let mut builder = opentelemetry_otlp::SpanExporter::builder()
                .with_http()
                .with_protocol(Protocol::HttpBinary)
                .with_endpoint(settings.otlp_endpoint.clone())
                .with_timeout(timeout);
            #[cfg(not(target_os = "solaris"))]
            {
                builder = builder.with_http_client(reqwest::blocking::Client::new());
            }
            builder.build()?
        }
        "grpc" => opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(settings.otlp_endpoint.clone())
            .with_timeout(timeout)
            .build()?,
        other => {
            anyhow::bail!(
                "unsupported OTLP protocol: {other:?}; expected \"http/protobuf\" or \"grpc\" or \"stdout\""
            );
        }
    };

    let span_processor = BatchSpanProcessor::builder(exporter)
        .with_batch_config(
            BatchConfigBuilder::default()
                .with_max_queue_size(2000)
                .with_scheduled_delay(Duration::from_secs(5))
                .build(),
        )
        .build();

    let provider = SdkTracerProvider::builder()
        .with_span_processor(span_processor)
        .with_resource(build_resource(settings))
        .build();

    Ok(provider)
}

pub fn init_tracer_provider(settings: &OtlpSettings) -> Result<SdkTracerProvider> {
    let provider = build_tracer_provider(settings)?;
    global::set_tracer_provider(provider.clone());
    Ok(provider)
}

fn build_resource(settings: &OtlpSettings) -> Resource {
    Resource::builder_empty()
        .with_attributes([
            KeyValue::new("service.name", settings.service_name.clone()),
            KeyValue::new("service.instance.id", settings.instance_id.clone()),
            KeyValue::new("host.name", hostname()),
            KeyValue::new("os.type", std::env::consts::OS.to_string()),
            KeyValue::new("host.arch", std::env::consts::ARCH.to_string()),
        ])
        .build()
}

pub fn hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| gethostname::gethostname().to_string_lossy().into_owned())
}

pub fn default_protocol_for_endpoint(endpoint: Option<&str>) -> String {
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
mod tests;
