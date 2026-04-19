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
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

pub const METRIC_PREFIX_SYSTEM: &str = "system.";
const ARCHIVE_SCHEMA_VERSION: &str = "v1";
const DEFAULT_ARCHIVE_WINDOW_SECS: u64 = 60;
const DEFAULT_TOP_K_LOG_SIGNATURES: usize = 3;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum ArchiveFormat {
    #[default]
    Parquet,
}

impl ArchiveFormat {
    pub fn parse(value: Option<&str>) -> Self {
        match value.map(|v| v.trim().to_ascii_lowercase()) {
            Some(v) if v == "parquet" || v.is_empty() => Self::Parquet,
            _ => Self::Parquet,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum ArchiveMode {
    #[default]
    Trend,
    Forensic,
    Lossless,
}

impl ArchiveMode {
    pub fn parse(value: Option<&str>) -> Self {
        match value.map(|v| v.trim().to_ascii_lowercase()) {
            Some(v) if v == "forensic" => Self::Forensic,
            Some(v) if v == "lossless" => Self::Lossless,
            _ => Self::Trend,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Trend => "trend",
            Self::Forensic => "forensic",
            Self::Lossless => "lossless",
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum ArchiveCompression {
    #[default]
    Zstd,
}

impl ArchiveCompression {
    pub fn parse(value: Option<&str>) -> Self {
        match value.map(|v| v.trim().to_ascii_lowercase()) {
            Some(v) if v == "zstd" || v.is_empty() => Self::Zstd,
            _ => Self::Zstd,
        }
    }

    fn to_parquet(&self) -> Compression {
        match self {
            Self::Zstd => Compression::ZSTD(ZstdLevel::try_new(3).unwrap_or_default()),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Zstd => "zstd",
        }
    }
}

#[derive(Clone, Debug)]
pub struct ArchiveStorageConfig {
    pub enabled: bool,
    pub archive_dir: String,
    pub max_file_bytes: u64,
    pub retain_files: usize,
    pub file_stem: String,
    pub format: ArchiveFormat,
    pub mode: ArchiveMode,
    pub window_secs: u64,
    pub compression: ArchiveCompression,
}

impl ArchiveStorageConfig {
    pub fn disabled(default_stem: &str) -> Self {
        Self {
            enabled: false,
            archive_dir: String::new(),
            max_file_bytes: 0,
            retain_files: 0,
            file_stem: default_stem.to_string(),
            format: ArchiveFormat::Parquet,
            mode: ArchiveMode::Trend,
            window_secs: DEFAULT_ARCHIVE_WINDOW_SECS,
            compression: ArchiveCompression::Zstd,
        }
    }
}

pub trait ArchiveWriter {
    fn write_snapshot(&mut self, value: &Value);
    fn write_log_batch(&mut self, values: &[Value]);
    fn is_healthy(&self) -> bool;
    fn total_records(&self) -> u64;
    fn total_bytes(&self) -> u64;
    fn last_error(&self) -> Option<&str>;
}

#[derive(Clone, Debug)]
struct TrendStats {
    min: f64,
    max: f64,
    sum: f64,
    count: u64,
    first: f64,
    last: f64,
}

impl TrendStats {
    fn new(value: f64) -> Self {
        Self {
            min: value,
            max: value,
            sum: value,
            count: 1,
            first: value,
            last: value,
        }
    }

    fn update(&mut self, value: f64) {
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
        self.sum += value;
        self.count = self.count.saturating_add(1);
        self.last = value;
    }

    fn avg(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TrendKey {
    service_name: String,
    instance_id: String,
    metric_key: String,
}

impl Hash for TrendKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.service_name.hash(state);
        self.instance_id.hash(state);
        self.metric_key.hash(state);
    }
}

#[derive(Clone, Debug)]
struct TrendRow {
    window_start_unix_secs: i64,
    service_name: String,
    instance_id: String,
    metric_key: String,
    min: f64,
    max: f64,
    avg: f64,
    count: u64,
    first: f64,
    last: f64,
    written_at_unix_ms: i64,
}

#[derive(Clone, Debug)]
struct ForensicRow {
    observed_unix_ms: i64,
    service_name: String,
    instance_id: String,
    record_type: String,
    severity: Option<String>,
    source: Option<String>,
    watch_target: Option<String>,
    message_signature: Option<u64>,
    sample_message: Option<String>,
    payload_json: String,
}

pub struct ParquetArchiveWriter {
    enabled: bool,
    dir: String,
    max_file_bytes: u64,
    retain_files: usize,
    file_stem: String,
    mode: ArchiveMode,
    window_secs: u64,
    compression: ArchiveCompression,
    pub total_records: u64,
    pub total_bytes: u64,
    pub healthy: bool,
    pub last_error: Option<String>,
    default_service_name: String,
    default_instance_id: String,
    trend_windows: BTreeMap<i64, HashMap<TrendKey, TrendStats>>,
}

impl ParquetArchiveWriter {
    pub fn from_config(config: &ArchiveStorageConfig) -> Self {
        Self {
            enabled: config.enabled,
            dir: config.archive_dir.clone(),
            max_file_bytes: config.max_file_bytes,
            retain_files: config.retain_files,
            file_stem: config.file_stem.clone(),
            mode: config.mode.clone(),
            window_secs: config.window_secs.max(1),
            compression: config.compression.clone(),
            total_records: 0,
            total_bytes: 0,
            healthy: true,
            last_error: None,
            default_service_name: String::new(),
            default_instance_id: String::new(),
            trend_windows: BTreeMap::new(),
        }
    }

    pub fn set_default_identity(&mut self, service_name: &str, instance_id: &str) {
        self.default_service_name = service_name.to_string();
        self.default_instance_id = instance_id.to_string();
    }

    pub fn write_json_line(&mut self, value: &Value) {
        self.write_snapshot(value);
    }

    pub fn flush(&mut self) {
        if !self.enabled {
            return;
        }
        if let ArchiveMode::Trend = self.mode {
            if let Err(err) = self.flush_all_trend_windows() {
                self.healthy = false;
                self.last_error = Some(err.to_string());
            }
        }
    }

    fn write_snapshot_impl(&mut self, value: &Value) -> anyhow::Result<()> {
        fs::create_dir_all(&self.dir)?;
        self.total_records = self.total_records.saturating_add(1);
        self.total_bytes = self
            .total_bytes
            .saturating_add((value.to_string().len() as u64).saturating_add(1));

        match self.mode {
            ArchiveMode::Trend => self.ingest_snapshot_to_trend(value),
            ArchiveMode::Forensic | ArchiveMode::Lossless => {
                self.write_forensic_rows(vec![self.build_forensic_row("snapshot", value)])
            }
        }
    }

    fn write_log_batch_impl(&mut self, values: &[Value]) -> anyhow::Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        fs::create_dir_all(&self.dir)?;
        self.total_records = self.total_records.saturating_add(values.len() as u64);
        self.total_bytes = self.total_bytes.saturating_add(
            values
                .iter()
                .map(|v| v.to_string().len() as u64 + 1)
                .sum::<u64>(),
        );

        match self.mode {
            ArchiveMode::Trend => self.ingest_logs_to_trend(values),
            ArchiveMode::Forensic | ArchiveMode::Lossless => {
                let rows = values
                    .iter()
                    .map(|v| self.build_forensic_row("log", v))
                    .collect::<Vec<_>>();
                self.write_forensic_rows(rows)
            }
        }
    }

    fn ingest_snapshot_to_trend(&mut self, value: &Value) -> anyhow::Result<()> {
        let now_secs = now_unix_secs();
        let window_start = align_window_start(now_secs, self.window_secs);
        let (service_name, instance_id) = self.resolve_identity(value);

        if let Some(obj) = value.as_object() {
            for (k, v) in obj {
                let mut path = vec![k.to_string()];
                self.flatten_numeric(v, &mut path, window_start, &service_name, &instance_id);
            }
        }

        self.flush_closed_windows(window_start)
    }

    fn ingest_logs_to_trend(&mut self, values: &[Value]) -> anyhow::Result<()> {
        let now_secs = now_unix_secs();
        let window_start = align_window_start(now_secs, self.window_secs);

        for value in values {
            let (service_name, instance_id) = self.resolve_identity(value);
            self.add_trend_point(
                window_start,
                TrendKey {
                    service_name: service_name.clone(),
                    instance_id: instance_id.clone(),
                    metric_key: "log.count".to_string(),
                },
                1.0,
            );

            if let Some(severity) = value.get("severity_text").and_then(Value::as_str) {
                self.add_trend_point(
                    window_start,
                    TrendKey {
                        service_name: service_name.clone(),
                        instance_id: instance_id.clone(),
                        metric_key: format!("log.severity.{}.count", sanitize_segment(severity)),
                    },
                    1.0,
                );
            }
            if let Some(source) = value.get("source").and_then(Value::as_str) {
                self.add_trend_point(
                    window_start,
                    TrendKey {
                        service_name: service_name.clone(),
                        instance_id: instance_id.clone(),
                        metric_key: format!("log.source.{}.count", sanitize_segment(source)),
                    },
                    1.0,
                );
            }
            if let Some(target) = value.get("watch_target").and_then(Value::as_str) {
                self.add_trend_point(
                    window_start,
                    TrendKey {
                        service_name: service_name.clone(),
                        instance_id: instance_id.clone(),
                        metric_key: format!("log.watch_target.{}.count", sanitize_segment(target)),
                    },
                    1.0,
                );
            }
        }

        let mut signatures = HashMap::<(String, String, u64), (u64, String)>::new();
        for value in values {
            if let Some(body) = value.get("body").and_then(Value::as_str) {
                let (service_name, instance_id) = self.resolve_identity(value);
                let sig = stable_signature(body);
                let entry = signatures
                    .entry((service_name, instance_id, sig))
                    .or_insert((0, body.to_string()));
                entry.0 = entry.0.saturating_add(1);
            }
        }

        let mut ranked = signatures.into_iter().collect::<Vec<_>>();
        ranked.sort_by_key(|entry| std::cmp::Reverse(entry.1 .0));
        for ((service_name, instance_id, sig), (count, _sample)) in
            ranked.into_iter().take(DEFAULT_TOP_K_LOG_SIGNATURES)
        {
            let metric_key = format!("log.topk.signature.{}.count", sig);
            self.add_trend_point(
                window_start,
                TrendKey {
                    service_name,
                    instance_id,
                    metric_key,
                },
                count as f64,
            );
        }

        self.flush_closed_windows(window_start)
    }

    fn flatten_numeric(
        &mut self,
        value: &Value,
        path: &mut Vec<String>,
        window_start: i64,
        service_name: &str,
        instance_id: &str,
    ) {
        match value {
            Value::Number(num) => {
                if let Some(v) = num.as_f64().filter(|candidate| candidate.is_finite()) {
                    self.add_trend_point(
                        window_start,
                        TrendKey {
                            service_name: service_name.to_string(),
                            instance_id: instance_id.to_string(),
                            metric_key: path.join("."),
                        },
                        v,
                    );
                }
            }
            Value::Object(map) => {
                for (k, v) in map {
                    path.push(k.to_string());
                    self.flatten_numeric(v, path, window_start, service_name, instance_id);
                    path.pop();
                }
            }
            Value::Array(values) => {
                for (idx, v) in values.iter().enumerate() {
                    path.push(idx.to_string());
                    self.flatten_numeric(v, path, window_start, service_name, instance_id);
                    path.pop();
                }
            }
            _ => {}
        }
    }

    fn add_trend_point(&mut self, window_start: i64, key: TrendKey, value: f64) {
        let map = self.trend_windows.entry(window_start).or_default();
        match map.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut existing) => {
                existing.get_mut().update(value);
            }
            std::collections::hash_map::Entry::Vacant(vacant) => {
                vacant.insert(TrendStats::new(value));
            }
        }
    }

    fn flush_closed_windows(&mut self, current_window: i64) -> anyhow::Result<()> {
        let closed = self
            .trend_windows
            .keys()
            .copied()
            .filter(|w| *w < current_window)
            .collect::<Vec<_>>();
        for window in closed {
            let values = self
                .trend_windows
                .remove(&window)
                .expect("closed window key collected from map must exist");
            self.write_trend_window(window, values)?;
        }
        Ok(())
    }

    fn flush_all_trend_windows(&mut self) -> anyhow::Result<()> {
        let windows = self.trend_windows.keys().copied().collect::<Vec<_>>();
        for window in windows {
            let values = self
                .trend_windows
                .remove(&window)
                .expect("window key collected from map must exist");
            self.write_trend_window(window, values)?;
        }
        Ok(())
    }

    fn write_trend_window(
        &mut self,
        window_start: i64,
        values: HashMap<TrendKey, TrendStats>,
    ) -> anyhow::Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let written_at = now_unix_millis();
        let rows = values
            .into_iter()
            .map(|(key, stats)| TrendRow {
                window_start_unix_secs: window_start,
                service_name: key.service_name,
                instance_id: key.instance_id,
                metric_key: key.metric_key,
                min: stats.min,
                max: stats.max,
                avg: stats.avg(),
                count: stats.count,
                first: stats.first,
                last: stats.last,
                written_at_unix_ms: written_at,
            })
            .collect::<Vec<_>>();

        self.write_trend_rows(rows)
    }

    fn write_trend_rows(&mut self, rows: Vec<TrendRow>) -> anyhow::Result<()> {
        let schema = trend_schema();

        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| r.window_start_unix_secs)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.service_name.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.instance_id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.metric_key.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.min).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.max).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.avg).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.count).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.first).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.last).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec![ARCHIVE_SCHEMA_VERSION; rows.len()])),
            Arc::new(StringArray::from(vec![self.mode.as_str(); rows.len()])),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| r.written_at_unix_ms)
                    .collect::<Vec<_>>(),
            )),
        ];

        let batch = RecordBatch::try_new(schema, columns)?;
        self.write_batch(batch, "trend")
    }

    fn build_forensic_row(&self, record_type: &str, value: &Value) -> ForensicRow {
        let (service_name, instance_id) = self.resolve_identity(value);
        let body = value
            .get("body")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);

        ForensicRow {
            observed_unix_ms: now_unix_millis(),
            service_name,
            instance_id,
            record_type: record_type.to_string(),
            severity: value
                .get("severity_text")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            source: value
                .get("source")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            watch_target: value
                .get("watch_target")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            message_signature: body.as_ref().map(|s| stable_signature(s)),
            sample_message: body,
            payload_json: value.to_string(),
        }
    }

    fn write_forensic_rows(&mut self, rows: Vec<ForensicRow>) -> anyhow::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let schema = forensic_schema();

        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.observed_unix_ms).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.service_name.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.instance_id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.record_type.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.severity.as_deref())
                    .collect::<Vec<Option<&str>>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.source.as_deref())
                    .collect::<Vec<Option<&str>>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.watch_target.as_deref())
                    .collect::<Vec<Option<&str>>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter()
                    .map(|r| r.message_signature)
                    .collect::<Vec<Option<u64>>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.sample_message.as_deref())
                    .collect::<Vec<Option<&str>>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.payload_json.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec![ARCHIVE_SCHEMA_VERSION; rows.len()])),
            Arc::new(StringArray::from(vec![self.mode.as_str(); rows.len()])),
        ];

        let batch = RecordBatch::try_new(schema, columns)?;
        let suffix = match self.mode {
            ArchiveMode::Lossless => "lossless",
            _ => "forensic",
        };
        self.write_batch(batch, suffix)
    }

    fn resolve_identity(&self, value: &Value) -> (String, String) {
        let (service_name, instance_id) = extract_identity(value);
        let resolved_service = if service_name.is_empty() {
            self.default_service_name.clone()
        } else {
            service_name
        };
        let resolved_instance = if instance_id.is_empty() {
            self.default_instance_id.clone()
        } else {
            instance_id
        };
        (resolved_service, resolved_instance)
    }

    fn write_batch(&mut self, batch: RecordBatch, suffix: &str) -> anyhow::Result<()> {
        let path = format!("{}/{}-{}.parquet", self.dir, self.file_stem, suffix);
        self.rotate_if_needed(&path)?;
        let existing_batches = self.read_existing_batches(&path, batch.schema())?;

        let mut metadata_builder = WriterProperties::builder();
        metadata_builder = metadata_builder
            .set_compression(self.compression.to_parquet())
            .set_created_by("ojo-archive".to_string());

        let props = metadata_builder.build();
        let file = File::create(&path)?;
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
        for existing in existing_batches {
            writer.write(&existing)?;
        }
        writer.write(&batch)?;
        writer.flush()?;
        writer.finish()?;
        self.prune_rotated_files(&path)?;
        Ok(())
    }

    fn read_existing_batches(
        &self,
        path: &str,
        expected_schema: Arc<Schema>,
    ) -> anyhow::Result<Vec<RecordBatch>> {
        if !Path::new(path).exists() {
            return Ok(Vec::new());
        }

        let file = File::open(path)?;
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
        let mut batches = Vec::new();
        for maybe_batch in &mut reader {
            let batch = maybe_batch?;
            if batch.schema().as_ref() != expected_schema.as_ref() {
                anyhow::bail!("existing archive schema mismatch for path: {path}");
            }
            batches.push(batch);
        }
        Ok(batches)
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

impl ArchiveWriter for ParquetArchiveWriter {
    fn write_snapshot(&mut self, value: &Value) {
        if !self.enabled {
            return;
        }
        if let Err(err) = self.write_snapshot_impl(value) {
            self.healthy = false;
            self.last_error = Some(err.to_string());
        } else {
            self.healthy = true;
            self.last_error = None;
        }
    }

    fn write_log_batch(&mut self, values: &[Value]) {
        if !self.enabled {
            return;
        }
        if let Err(err) = self.write_log_batch_impl(values) {
            self.healthy = false;
            self.last_error = Some(err.to_string());
        } else {
            self.healthy = true;
            self.last_error = None;
        }
    }

    fn is_healthy(&self) -> bool {
        self.healthy
    }

    fn total_records(&self) -> u64 {
        self.total_records
    }

    fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }
}

impl Drop for ParquetArchiveWriter {
    fn drop(&mut self) {
        self.flush();
    }
}

pub type JsonArchiveWriter = ParquetArchiveWriter;

fn trend_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("window_start_unix_secs", DataType::Int64, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("instance_id", DataType::Utf8, false),
        Field::new("metric_key", DataType::Utf8, false),
        Field::new("min", DataType::Float64, false),
        Field::new("max", DataType::Float64, false),
        Field::new("avg", DataType::Float64, false),
        Field::new("count", DataType::UInt64, false),
        Field::new("first", DataType::Float64, false),
        Field::new("last", DataType::Float64, false),
        Field::new("archive_schema_version", DataType::Utf8, false),
        Field::new("archive_mode", DataType::Utf8, false),
        Field::new("written_at_unix_ms", DataType::Int64, false),
    ]))
}

fn forensic_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("observed_unix_ms", DataType::Int64, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("instance_id", DataType::Utf8, false),
        Field::new("record_type", DataType::Utf8, false),
        Field::new("severity", DataType::Utf8, true),
        Field::new("source", DataType::Utf8, true),
        Field::new("watch_target", DataType::Utf8, true),
        Field::new("message_signature", DataType::UInt64, true),
        Field::new("sample_message", DataType::Utf8, true),
        Field::new("payload_json", DataType::Utf8, false),
        Field::new("archive_schema_version", DataType::Utf8, false),
        Field::new("archive_mode", DataType::Utf8, false),
    ]))
}

fn sanitize_segment(raw: &str) -> String {
    let lowered = raw.to_ascii_lowercase();
    let cleaned = lowered
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();

    cleaned.trim_matches('_').to_string()
}

fn stable_signature(value: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn now_unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn now_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn align_window_start(timestamp_secs: i64, window_secs: u64) -> i64 {
    let ws = window_secs.max(1) as i64;
    (timestamp_secs / ws) * ws
}

fn extract_identity(value: &Value) -> (String, String) {
    let service_name = value
        .get("service_name")
        .and_then(Value::as_str)
        .or_else(|| {
            value
                .get("service")
                .and_then(Value::as_object)
                .and_then(|m| m.get("name"))
                .and_then(Value::as_str)
        })
        .unwrap_or_default()
        .to_string();

    let instance_id = value
        .get("instance_id")
        .and_then(Value::as_str)
        .or_else(|| {
            value
                .get("service")
                .and_then(Value::as_object)
                .and_then(|m| m.get("instance_id"))
                .and_then(Value::as_str)
        })
        .unwrap_or_default()
        .to_string();

    (service_name, instance_id)
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
