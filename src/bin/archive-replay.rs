use anyhow::{anyhow, Context, Result};
use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use prost::Message;
use reqwest::blocking::Client;
use serde_json::Value;
use snap::raw::Encoder;
use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
struct ReplayPoint {
    timestamp_ms: i64,
    service_name: String,
    instance_id: String,
    metric_key: String,
    aggregation: String,
    value: f64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Protocol {
    Otlp,
    RemoteWrite,
    ShellLogs,
}

#[derive(Clone, Debug)]
struct Config {
    archive_dir: PathBuf,
    endpoint: Option<String>,
    protocol: Protocol,
    batch_size: usize,
    service_name: String,
}

type SeriesKey = (String, String, String, String);
type SamplePoint = (i64, f64);

#[derive(Clone, PartialEq, Message)]
struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    timeseries: Vec<TimeSeries>,
}

#[derive(Clone, PartialEq, Message)]
struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    samples: Vec<Sample>,
}

#[derive(Clone, PartialEq, Message)]
struct Label {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    value: String,
}

#[derive(Clone, PartialEq, Message)]
struct Sample {
    #[prost(double, tag = "1")]
    value: f64,
    #[prost(int64, tag = "2")]
    timestamp: i64,
}

fn main() -> Result<()> {
    let cfg = parse_args(env::args().collect())?;
    let files = discover_parquet_files(&cfg.archive_dir)?;
    if files.is_empty() {
        return Err(anyhow!(
            "no parquet archives found under '{}'",
            cfg.archive_dir.display()
        ));
    }

    let mut points = Vec::new();
    for file in files {
        points.extend(read_replay_points(&file)?);
    }

    if points.is_empty() {
        return Err(anyhow!("no replayable rows found in archive parquet files"));
    }

    match cfg.protocol {
        Protocol::ShellLogs => replay_shell_logs(&cfg, &points),
        Protocol::Otlp => {
            let client = Client::builder()
                .timeout(std::time::Duration::from_secs(20))
                .build()
                .context("failed to build HTTP client")?;
            replay_otlp(&client, &cfg, &points)
        }
        Protocol::RemoteWrite => {
            let client = Client::builder()
                .timeout(std::time::Duration::from_secs(20))
                .build()
                .context("failed to build HTTP client")?;
            replay_remote_write(&client, &cfg, &points)
        }
    }
}

fn parse_args(args: Vec<String>) -> Result<Config> {
    let mut archive_dir = None;
    let mut endpoint = None;
    let mut protocol = Protocol::Otlp;
    let mut batch_size = 500usize;
    let mut service_name = "ojo-archive-replay".to_string();

    let mut i = 1usize;
    while i < args.len() {
        match args[i].as_str() {
            "--archive-dir" => {
                i += 1;
                archive_dir = args.get(i).cloned();
            }
            "--endpoint" => {
                i += 1;
                endpoint = args.get(i).cloned();
            }
            "--protocol" => {
                i += 1;
                let raw = args
                    .get(i)
                    .ok_or_else(|| anyhow!("missing value for --protocol"))?;
                protocol = parse_protocol(raw)?;
            }
            "--batch-size" => {
                i += 1;
                batch_size = args
                    .get(i)
                    .ok_or_else(|| anyhow!("missing value for --batch-size"))?
                    .parse::<usize>()
                    .context("invalid --batch-size")?
                    .max(1);
            }
            "--service-name" => {
                i += 1;
                service_name = args
                    .get(i)
                    .cloned()
                    .ok_or_else(|| anyhow!("missing value for --service-name"))?;
            }
            other => {
                return Err(anyhow!(
                    "unknown arg '{}'; expected --archive-dir [--endpoint] [--protocol] [--batch-size] [--service-name]",
                    other
                ));
            }
        }
        i += 1;
    }

    if !matches!(protocol, Protocol::ShellLogs) && endpoint.is_none() {
        return Err(anyhow!("--endpoint is required for otlp and remote-write"));
    }

    Ok(Config {
        archive_dir: archive_dir
            .map(PathBuf::from)
            .ok_or_else(|| anyhow!("--archive-dir is required"))?,
        endpoint,
        protocol,
        batch_size,
        service_name,
    })
}

fn parse_protocol(raw: &str) -> Result<Protocol> {
    match raw {
        "otlp" => Ok(Protocol::Otlp),
        "remote-write" => Ok(Protocol::RemoteWrite),
        "shell-logs" | "stdout" => Ok(Protocol::ShellLogs),
        other => Err(anyhow!(
            "unsupported protocol '{}'; use 'otlp', 'remote-write', or 'shell-logs'",
            other
        )),
    }
}

fn discover_parquet_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir).with_context(|| format!("read_dir {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "parquet") {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

fn read_replay_points(path: &Path) -> Result<Vec<ReplayPoint>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("open parquet {}", path.display()))?
        .build()
        .with_context(|| format!("build reader {}", path.display()))?;

    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.with_context(|| format!("read batch {}", path.display()))?;
        if is_trend_batch(&batch) {
            out.extend(parse_trend_batch(&batch)?);
        } else if is_row_batch(&batch) {
            out.extend(parse_row_batch(&batch)?);
        }
    }
    Ok(out)
}

fn is_trend_batch(batch: &RecordBatch) -> bool {
    has_column(batch, "window_start_unix_secs")
        && has_column(batch, "metric_key")
        && has_column(batch, "avg")
        && has_column(batch, "min")
        && has_column(batch, "max")
        && has_column(batch, "last")
}

fn is_row_batch(batch: &RecordBatch) -> bool {
    has_column(batch, "observed_unix_ms")
        && has_column(batch, "record_type")
        && has_column(batch, "payload_json")
}

fn has_column(batch: &RecordBatch, name: &str) -> bool {
    batch.schema().column_with_name(name).is_some()
}

fn parse_trend_batch(batch: &RecordBatch) -> Result<Vec<ReplayPoint>> {
    let win = col_i64(batch, "window_start_unix_secs")?;
    let svc = col_utf8(batch, "service_name")?;
    let inst = col_utf8(batch, "instance_id")?;
    let key = col_utf8(batch, "metric_key")?;
    let avg = col_f64(batch, "avg")?;
    let min = col_f64(batch, "min")?;
    let max = col_f64(batch, "max")?;
    let last = col_f64(batch, "last")?;

    let mut out = Vec::with_capacity(batch.num_rows() * 4);
    for idx in 0..batch.num_rows() {
        if win.is_null(idx)
            || svc.is_null(idx)
            || inst.is_null(idx)
            || key.is_null(idx)
            || avg.is_null(idx)
            || min.is_null(idx)
            || max.is_null(idx)
            || last.is_null(idx)
        {
            continue;
        }
        let timestamp_ms = win.value(idx).saturating_mul(1000);
        let service_name = svc.value(idx).to_string();
        let instance_id = inst.value(idx).to_string();
        let metric_key = key.value(idx).to_string();

        for (aggregation, value) in [
            ("avg".to_string(), avg.value(idx)),
            ("min".to_string(), min.value(idx)),
            ("max".to_string(), max.value(idx)),
            ("last".to_string(), last.value(idx)),
        ] {
            out.push(ReplayPoint {
                timestamp_ms,
                service_name: service_name.clone(),
                instance_id: instance_id.clone(),
                metric_key: metric_key.clone(),
                aggregation,
                value,
            });
        }
    }
    Ok(out)
}

fn parse_row_batch(batch: &RecordBatch) -> Result<Vec<ReplayPoint>> {
    let observed = col_i64(batch, "observed_unix_ms")?;
    let service = col_utf8(batch, "service_name")?;
    let instance = col_utf8(batch, "instance_id")?;
    let record_type = col_utf8(batch, "record_type")?;
    let payload_json = col_utf8(batch, "payload_json")?;
    let severity = col_optional_utf8(batch, "severity");
    let source = col_optional_utf8(batch, "source");
    let watch_target = col_optional_utf8(batch, "watch_target");

    let mut out = Vec::new();
    for idx in 0..batch.num_rows() {
        if observed.is_null(idx)
            || service.is_null(idx)
            || instance.is_null(idx)
            || record_type.is_null(idx)
            || payload_json.is_null(idx)
        {
            continue;
        }

        let timestamp_ms = observed.value(idx);
        let service_name = service.value(idx).to_string();
        let instance_id = instance.value(idx).to_string();
        let rtype = record_type.value(idx);
        let payload = payload_json.value(idx);

        out.push(ReplayPoint {
            timestamp_ms,
            service_name: service_name.clone(),
            instance_id: instance_id.clone(),
            metric_key: "archive.rows.total".to_string(),
            aggregation: "count".to_string(),
            value: 1.0,
        });

        if rtype == "log" {
            out.push(ReplayPoint {
                timestamp_ms,
                service_name: service_name.clone(),
                instance_id: instance_id.clone(),
                metric_key: "archive.logs.total".to_string(),
                aggregation: "count".to_string(),
                value: 1.0,
            });
            if let Some(arr) = severity.as_ref().and_then(|a| value_at(a, idx)) {
                out.push(ReplayPoint {
                    timestamp_ms,
                    service_name: service_name.clone(),
                    instance_id: instance_id.clone(),
                    metric_key: format!("archive.logs.severity.{}.total", sanitize_segment(arr)),
                    aggregation: "count".to_string(),
                    value: 1.0,
                });
            }
            if let Some(arr) = source.as_ref().and_then(|a| value_at(a, idx)) {
                out.push(ReplayPoint {
                    timestamp_ms,
                    service_name: service_name.clone(),
                    instance_id: instance_id.clone(),
                    metric_key: format!("archive.logs.source.{}.total", sanitize_segment(arr)),
                    aggregation: "count".to_string(),
                    value: 1.0,
                });
            }
            if let Some(arr) = watch_target.as_ref().and_then(|a| value_at(a, idx)) {
                out.push(ReplayPoint {
                    timestamp_ms,
                    service_name: service_name.clone(),
                    instance_id: instance_id.clone(),
                    metric_key: format!(
                        "archive.logs.watch_target.{}.total",
                        sanitize_segment(arr)
                    ),
                    aggregation: "count".to_string(),
                    value: 1.0,
                });
            }
        }

        match serde_json::from_str::<Value>(payload) {
            Ok(value) => {
                let mut numeric = Vec::new();
                let mut path = Vec::new();
                flatten_numeric(&value, &mut path, &mut numeric);
                for (metric_key, val) in numeric {
                    out.push(ReplayPoint {
                        timestamp_ms,
                        service_name: service_name.clone(),
                        instance_id: instance_id.clone(),
                        metric_key: format!("payload.{}", metric_key),
                        aggregation: "value".to_string(),
                        value: val,
                    });
                }
            }
            Err(_) => {
                out.push(ReplayPoint {
                    timestamp_ms,
                    service_name,
                    instance_id,
                    metric_key: "archive.payload.parse_errors.total".to_string(),
                    aggregation: "count".to_string(),
                    value: 1.0,
                });
            }
        }
    }

    Ok(out)
}

fn flatten_numeric(value: &Value, path: &mut Vec<String>, out: &mut Vec<(String, f64)>) {
    match value {
        Value::Number(num) => {
            if let Some(v) = num.as_f64() {
                if v.is_finite() {
                    out.push((path.join("."), v));
                }
            }
        }
        Value::Object(map) => {
            for (k, v) in map {
                path.push(k.to_string());
                flatten_numeric(v, path, out);
                path.pop();
            }
        }
        Value::Array(values) => {
            for (idx, v) in values.iter().enumerate() {
                path.push(idx.to_string());
                flatten_numeric(v, path, out);
                path.pop();
            }
        }
        _ => {}
    }
}

fn value_at(arr: &StringArray, idx: usize) -> Option<&str> {
    if arr.is_null(idx) {
        None
    } else {
        Some(arr.value(idx))
    }
}

fn col_index(batch: &RecordBatch, name: &str) -> Result<usize> {
    batch
        .schema()
        .column_with_name(name)
        .map(|(idx, _)| idx)
        .ok_or_else(|| anyhow!("missing column '{}'", name))
}

fn col_utf8<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    let idx = col_index(batch, name)?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("column '{}' is not utf8", name))
}

fn col_optional_utf8<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a StringArray> {
    let idx = batch.schema().column_with_name(name).map(|(idx, _)| idx)?;
    batch.column(idx).as_any().downcast_ref::<StringArray>()
}

fn col_i64<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int64Array> {
    let idx = col_index(batch, name)?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("column '{}' is not int64", name))
}

fn col_f64<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Float64Array> {
    let idx = col_index(batch, name)?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| anyhow!("column '{}' is not float64", name))
}

fn replay_otlp(client: &Client, cfg: &Config, points: &[ReplayPoint]) -> Result<()> {
    let endpoint = cfg
        .endpoint
        .as_deref()
        .ok_or_else(|| anyhow!("missing endpoint for otlp replay"))?;
    for chunk in points.chunks(cfg.batch_size) {
        let metrics = chunk
            .iter()
            .filter_map(|p| {
                if p.timestamp_ms < 0 {
                    return None;
                }
                let time_unix_nano = (p.timestamp_ms as u64).saturating_mul(1_000_000);
                Some(serde_json::json!({
                    "name": format!("archive.{}.{}", sanitize_metric_name(&p.metric_key), sanitize_metric_name(&p.aggregation)),
                    "gauge": {
                        "dataPoints": [{
                            "timeUnixNano": time_unix_nano.to_string(),
                            "asDouble": p.value,
                            "attributes": [
                                {"key": "metric_key", "value": {"stringValue": p.metric_key}},
                                {"key": "service_name", "value": {"stringValue": p.service_name}},
                                {"key": "instance_id", "value": {"stringValue": p.instance_id}},
                                {"key": "aggregation", "value": {"stringValue": p.aggregation}},
                            ]
                        }]
                    }
                }))
            })
            .collect::<Vec<_>>();

        if metrics.is_empty() {
            continue;
        }

        let payload = serde_json::json!({
            "resourceMetrics": [{
                "resource": {
                    "attributes": [{
                        "key": "service.name",
                        "value": {"stringValue": cfg.service_name}
                    }]
                },
                "scopeMetrics": [{
                    "scope": {"name": "ojo.archive.replay"},
                    "metrics": metrics
                }]
            }]
        });

        let body = serde_json::to_vec(&payload).context("serialize OTLP payload")?;
        let response = client
            .post(endpoint)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .with_context(|| format!("send OTLP replay batch to {}", endpoint))?;
        if !response.status().is_success() {
            return Err(anyhow!(
                "OTLP replay failed with status {}",
                response.status()
            ));
        }
    }

    Ok(())
}

fn replay_remote_write(client: &Client, cfg: &Config, points: &[ReplayPoint]) -> Result<()> {
    let endpoint = cfg
        .endpoint
        .as_deref()
        .ok_or_else(|| anyhow!("missing endpoint for remote-write replay"))?;
    let mut series_map: HashMap<SeriesKey, Vec<SamplePoint>> = HashMap::new();

    for point in points {
        let key = (
            point.service_name.clone(),
            point.instance_id.clone(),
            point.metric_key.clone(),
            point.aggregation.clone(),
        );
        series_map
            .entry(key)
            .or_default()
            .push((point.timestamp_ms, point.value));
    }

    let mut timeseries = Vec::new();
    for ((service_name, instance_id, metric_key, agg), mut samples) in series_map {
        samples.sort_by_key(|(ts, _)| *ts);
        let labels = vec![
            Label {
                name: "__name__".to_string(),
                value: format!(
                    "{}_archive_{}",
                    sanitize_metric_name(&service_name),
                    sanitize_metric_name(&agg)
                ),
            },
            Label {
                name: "service_name".to_string(),
                value: service_name,
            },
            Label {
                name: "instance_id".to_string(),
                value: instance_id,
            },
            Label {
                name: "metric_key".to_string(),
                value: metric_key,
            },
        ];
        let samples = samples
            .into_iter()
            .map(|(timestamp, value)| Sample { value, timestamp })
            .collect::<Vec<_>>();

        timeseries.push(TimeSeries { labels, samples });
    }

    let body = WriteRequest { timeseries }.encode_to_vec();
    let compressed = Encoder::new()
        .compress_vec(&body)
        .context("snappy compress remote-write body")?;

    let response = client
        .post(endpoint)
        .header("content-encoding", "snappy")
        .header("content-type", "application/x-protobuf")
        .header("x-prometheus-remote-write-version", "0.1.0")
        .body(compressed)
        .send()
        .with_context(|| format!("send remote-write replay batch to {}", endpoint))?;

    if !response.status().is_success() {
        return Err(anyhow!(
            "remote-write replay failed with status {}",
            response.status()
        ));
    }

    Ok(())
}

fn replay_shell_logs(cfg: &Config, points: &[ReplayPoint]) -> Result<()> {
    let mut ordered = points.to_vec();
    ordered.sort_by(|a, b| a.timestamp_ms.cmp(&b.timestamp_ms));

    println!(
        "archive replay shell logs: points={} service={}",
        ordered.len(),
        cfg.service_name
    );
    for point in ordered {
        println!(
            "ts_ms={} service={} instance={} metric_key={} aggregation={} value={}",
            point.timestamp_ms,
            point.service_name,
            point.instance_id,
            point.metric_key,
            point.aggregation,
            point.value
        );
    }
    Ok(())
}

fn sanitize_metric_name(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == ':' {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        return "unknown".to_string();
    }
    if !out.chars().next().unwrap_or('a').is_ascii_alphabetic() && !out.starts_with('_') {
        out.insert(0, '_');
    }
    out
}

fn sanitize_segment(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        "unknown".to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{flatten_numeric, parse_protocol, sanitize_segment, Protocol, ReplayPoint};
    use serde_json::json;

    #[test]
    fn flatten_numeric_collects_nested_fields() {
        let value = json!({
            "a": 1,
            "b": {"c": 2.5},
            "d": [3, "x", {"e": 4}],
            "f": true
        });
        let mut out = Vec::new();
        let mut path = Vec::new();
        flatten_numeric(&value, &mut path, &mut out);
        out.sort_by(|l, r| l.0.cmp(&r.0));

        assert_eq!(out.len(), 4);
        assert_eq!(out[0], ("a".to_string(), 1.0));
        assert_eq!(out[1], ("b.c".to_string(), 2.5));
        assert_eq!(out[2], ("d.0".to_string(), 3.0));
        assert_eq!(out[3], ("d.2.e".to_string(), 4.0));
    }

    #[test]
    fn sanitize_segment_normalizes_strings() {
        assert_eq!(sanitize_segment("Warn/Err"), "warn_err");
        assert_eq!(sanitize_segment("***"), "unknown");
    }

    #[test]
    fn replay_point_shape_is_constructible() {
        let point = ReplayPoint {
            timestamp_ms: 1,
            service_name: "svc".to_string(),
            instance_id: "inst".to_string(),
            metric_key: "k".to_string(),
            aggregation: "avg".to_string(),
            value: 1.2,
        };
        assert_eq!(point.service_name, "svc");
        assert_eq!(point.value, 1.2);
    }

    #[test]
    fn parse_protocol_accepts_shell_logs_aliases() {
        assert!(matches!(parse_protocol("otlp"), Ok(Protocol::Otlp)));
        assert!(matches!(
            parse_protocol("remote-write"),
            Ok(Protocol::RemoteWrite)
        ));
        assert!(matches!(
            parse_protocol("shell-logs"),
            Ok(Protocol::ShellLogs)
        ));
        assert!(matches!(parse_protocol("stdout"), Ok(Protocol::ShellLogs)));
        assert!(parse_protocol("invalid").is_err());
    }
}
