use crate::config::TieredReplayConfig;
use crate::model::Snapshot;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug)]
pub struct BufferedInterval {
    pub snapshot: Snapshot,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedInterval {
    snapshot: Snapshot,
}

#[derive(Clone, Debug)]
struct SegmentMeta {
    path: PathBuf,
    created_at_unix_secs: u64,
    bytes: u64,
}

#[derive(Clone, Debug)]
pub struct TieredReplayQueue {
    enabled: bool,
    memory_cap_items: usize,
    memory: VecDeque<BufferedInterval>,
    wal_dir: PathBuf,
    wal_segment_max_bytes: u64,
    wal_segment_max_age: Duration,
    active_segment: Option<SegmentMeta>,
    next_segment_id: u64,
}

impl TieredReplayQueue {
    pub fn from_config(config: &TieredReplayConfig) -> Result<Self> {
        let wal_dir = PathBuf::from(&config.wal_dir);
        let mut queue = Self {
            enabled: config.enabled,
            memory_cap_items: config.memory_cap_items.max(1),
            memory: VecDeque::new(),
            wal_dir,
            wal_segment_max_bytes: config.wal_segment_max_bytes.max(1024),
            wal_segment_max_age: Duration::from_secs(config.wal_segment_max_age_secs.max(1)),
            active_segment: None,
            next_segment_id: 1,
        };

        if queue.enabled {
            fs::create_dir_all(&queue.wal_dir).with_context(|| {
                format!("failed to create WAL directory {}", queue.wal_dir.display())
            })?;
            queue.next_segment_id = queue.discover_next_segment_id()?;
        }

        Ok(queue)
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn push(&mut self, interval: BufferedInterval) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        self.memory.push_back(interval);
        while self.memory.len() > self.memory_cap_items {
            let oldest = self
                .memory
                .pop_front()
                .expect("memory length checked before spill");
            self.spill_to_wal(&oldest)?;
        }
        Ok(())
    }

    pub fn drain_batch(&mut self, max_items: usize) -> Result<Vec<BufferedInterval>> {
        let mut out = Vec::new();
        let target = max_items.max(1);

        for _ in 0..target {
            let Some(item) = self.pop_next_interval()? else {
                break;
            };
            out.push(item);
        }

        Ok(out)
    }

    pub fn requeue_front(&mut self, mut items: Vec<BufferedInterval>) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        while let Some(item) = items.pop() {
            self.memory.push_front(item);
        }

        while self.memory.len() > self.memory_cap_items {
            let oldest = self
                .memory
                .pop_front()
                .expect("memory length checked before spill");
            self.spill_to_wal(&oldest)?;
        }

        Ok(())
    }

    pub fn has_pending(&self) -> bool {
        if !self.enabled {
            return false;
        }
        if !self.memory.is_empty() {
            return true;
        }

        self.scan_segments().map(|v| !v.is_empty()).unwrap_or(false)
    }

    fn pop_next_interval(&mut self) -> Result<Option<BufferedInterval>> {
        if !self.enabled {
            return Ok(None);
        }

        if !self.scan_segments()?.is_empty() {
            self.rehydrate_from_oldest_segment()?;
        }

        if let Some(item) = self.memory.pop_front() {
            return Ok(Some(item));
        }

        self.rehydrate_from_oldest_segment()?;
        Ok(self.memory.pop_front())
    }

    fn spill_to_wal(&mut self, interval: &BufferedInterval) -> Result<()> {
        let payload = serde_json::to_string(&PersistedInterval {
            snapshot: interval.snapshot.clone(),
        })
        .context("failed to serialize buffered interval")?;
        let line = format!("{payload}\n");
        let line_len = line.len() as u64;

        self.rotate_active_if_needed()?;
        let active = self.ensure_active_segment()?;

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&active.path)
            .with_context(|| format!("failed to open WAL segment {}", active.path.display()))?;
        file.write_all(line.as_bytes())
            .with_context(|| format!("failed to write WAL segment {}", active.path.display()))?;
        file.flush()
            .with_context(|| format!("failed to flush WAL segment {}", active.path.display()))?;
        active.bytes = active.bytes.saturating_add(line_len);

        if active.bytes >= self.wal_segment_max_bytes {
            self.active_segment = None;
        }

        Ok(())
    }

    fn rotate_active_if_needed(&mut self) -> Result<()> {
        let Some(active) = &self.active_segment else {
            return Ok(());
        };

        let now_secs = now_unix_secs();
        let age = now_secs.saturating_sub(active.created_at_unix_secs);
        if age >= self.wal_segment_max_age.as_secs() {
            self.active_segment = None;
            return Ok(());
        }

        let meta = fs::metadata(&active.path)
            .with_context(|| format!("failed to stat WAL segment {}", active.path.display()))?;
        if meta.len() >= self.wal_segment_max_bytes {
            self.active_segment = None;
        }

        Ok(())
    }

    fn ensure_active_segment(&mut self) -> Result<&mut SegmentMeta> {
        if self.active_segment.is_none() {
            let id = self.next_segment_id;
            self.next_segment_id = self.next_segment_id.saturating_add(1);
            let path = self.wal_dir.join(segment_file_name(id));
            self.active_segment = Some(SegmentMeta {
                path,
                created_at_unix_secs: now_unix_secs(),
                bytes: 0,
            });
        }

        Ok(self
            .active_segment
            .as_mut()
            .expect("active segment must exist after initialization"))
    }

    fn discover_next_segment_id(&self) -> Result<u64> {
        let mut max_id = 0u64;
        for path in self.scan_segments()? {
            if let Some(id) = parse_segment_id(&path) {
                if id > max_id {
                    max_id = id;
                }
            }
        }
        Ok(max_id.saturating_add(1))
    }

    fn rehydrate_from_oldest_segment(&mut self) -> Result<()> {
        let mut segments = self.scan_segments()?;
        if segments.is_empty() {
            return Ok(());
        }

        segments.sort();
        let oldest = segments[0].clone();

        // If the active writer is the oldest file, close it before reading so
        // replay can progress even when no new writes arrive.
        if self
            .active_segment
            .as_ref()
            .is_some_and(|meta| meta.path == oldest)
        {
            self.active_segment = None;
        }

        let file = File::open(&oldest)
            .with_context(|| format!("failed to open WAL segment {}", oldest.display()))?;
        let reader = BufReader::new(file);

        let mut loaded = Vec::new();
        for line in reader.lines() {
            let line =
                line.with_context(|| format!("failed to read WAL line {}", oldest.display()))?;
            if line.trim().is_empty() {
                continue;
            }
            let persisted: PersistedInterval = serde_json::from_str(&line)
                .with_context(|| format!("failed to decode WAL line {}", oldest.display()))?;
            loaded.push(BufferedInterval {
                snapshot: persisted.snapshot,
            });
        }

        for interval in loaded.into_iter().rev() {
            self.memory.push_front(interval);
        }

        fs::remove_file(&oldest).with_context(|| {
            format!(
                "failed to remove compacted WAL segment {}",
                oldest.display()
            )
        })?;
        Ok(())
    }

    fn scan_segments(&self) -> Result<Vec<PathBuf>> {
        if !self.wal_dir.exists() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for entry in fs::read_dir(&self.wal_dir)
            .with_context(|| format!("failed to read WAL directory {}", self.wal_dir.display()))?
        {
            let path = entry?.path();
            if path.is_file() && parse_segment_id(&path).is_some() {
                out.push(path);
            }
        }
        Ok(out)
    }
}

fn segment_file_name(id: u64) -> String {
    format!("segment-{id:020}.wal")
}

fn parse_segment_id(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let rest = name.strip_prefix("segment-")?;
    let id = rest.strip_suffix(".wal")?;
    id.parse::<u64>().ok()
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
#[path = "tests/tiered_queue_tests.rs"]
mod tests;
