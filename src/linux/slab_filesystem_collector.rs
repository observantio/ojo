fn collect_slabinfo() -> Result<BTreeMap<String, u64>> {
    let Ok(contents) = fs::read_to_string("/proc/slabinfo") else {
        return Ok(collect_slabinfo_sysfs());
    };
    let out = parse_slabinfo_text(&contents);

    if out.is_empty() {
        Ok(collect_slabinfo_sysfs())
    } else {
        Ok(out)
    }
}

fn parse_slabinfo_text(contents: &str) -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();

    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let cols = trimmed.split_whitespace().collect::<Vec<_>>();
        if cols.len() < 6 {
            continue;
        }

        let name = cols[0];
        let active_objs = cols[1].parse::<u64>().unwrap_or(0);
        let num_objs = cols[2].parse::<u64>().unwrap_or(0);
        let objsize = cols[3].parse::<u64>().unwrap_or(0);
        let objperslab = cols[4].parse::<u64>().unwrap_or(0);
        let pagesperslab = cols[5].parse::<u64>().unwrap_or(0);

        out.insert(key_pipe3(name, "active_objs", "value"), active_objs);
        out.insert(key_pipe3(name, "num_objs", "value"), num_objs);
        out.insert(key_pipe3(name, "objsize", "bytes"), objsize);
        out.insert(key_pipe3(name, "objperslab", "value"), objperslab);
        out.insert(key_pipe3(name, "pagesperslab", "value"), pagesperslab);

        if let Some(pos) = cols.iter().position(|c| *c == ":") {
            if cols.len() >= pos + 4 {
                let active_slabs = cols[pos + 1].parse::<u64>().unwrap_or(0);
                let num_slabs = cols[pos + 2].parse::<u64>().unwrap_or(0);
                out.insert(key_pipe3(name, "active_slabs", "value"), active_slabs);
                out.insert(key_pipe3(name, "num_slabs", "value"), num_slabs);
            }
        }
    }

    out
}

fn collect_slabinfo_sysfs() -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();
    let Ok(entries) = fs::read_dir("/sys/kernel/slab") else {
        return out;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|v| v.to_str()) else {
            continue;
        };

        let read_u64 = |file: &str| -> Option<u64> {
            let raw = fs::read_to_string(path.join(file)).ok()?;
            raw.split_whitespace().next()?.parse::<u64>().ok()
        };

        if let Some(value) = read_u64("objects") {
            out.insert(key_pipe3(name, "active_objs", "value"), value);
        }
        if let Some(value) = read_u64("total_objects") {
            out.insert(key_pipe3(name, "num_objs", "value"), value);
        }
        if let Some(value) = read_u64("object_size") {
            out.insert(key_pipe3(name, "objsize", "bytes"), value);
        }
        if let Some(value) = read_u64("objects_per_slab") {
            out.insert(key_pipe3(name, "objperslab", "value"), value);
        }
        if let Some(value) = read_u64("slabs") {
            out.insert(key_pipe3(name, "num_slabs", "value"), value);
        }
        if let Some(value) = read_u64("partial") {
            out.insert(key_pipe3(name, "partial_slabs", "value"), value);
        }
    }

    out
}

fn collect_filesystem_stats(mounts: &[MountSnapshot]) -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();

    for mount in mounts {
        let mountpoint = Path::new(&mount.mountpoint);
        let Ok(path_c) = CString::new(mountpoint.as_os_str().as_bytes()) else {
            continue;
        };

        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::statvfs(path_c.as_ptr(), &mut stat) };
        if rc != 0 {
            continue;
        }

        let frsize = if stat.f_frsize > 0 {
            stat.f_frsize
        } else {
            stat.f_bsize
        };

        let total_bytes = stat.f_blocks.saturating_mul(frsize);
        let free_bytes = stat.f_bfree.saturating_mul(frsize);
        let avail_bytes = stat.f_bavail.saturating_mul(frsize);
        let used_bytes = total_bytes.saturating_sub(free_bytes);
        let used_bytes_user_visible = total_bytes.saturating_sub(avail_bytes);
        let reserved_bytes = free_bytes.saturating_sub(avail_bytes);

        out.insert(
            key_pipe3(&mount.mountpoint, "total_bytes", "value"),
            total_bytes,
        );
        out.insert(
            key_pipe3(&mount.mountpoint, "used_bytes", "value"),
            used_bytes,
        );
        out.insert(
            key_pipe3(&mount.mountpoint, "used_bytes_user_visible", "value"),
            used_bytes_user_visible,
        );
        out.insert(
            key_pipe3(&mount.mountpoint, "reserved_bytes", "value"),
            reserved_bytes,
        );
        out.insert(
            key_pipe3(&mount.mountpoint, "free_bytes", "value"),
            free_bytes,
        );
        out.insert(
            key_pipe3(&mount.mountpoint, "avail_bytes", "value"),
            avail_bytes,
        );
        out.insert(
            key_pipe3(&mount.mountpoint, "blocks", "value"),
            stat.f_blocks,
        );
        out.insert(
            key_pipe3(&mount.mountpoint, "bfree", "value"),
            stat.f_bfree,
        );
        out.insert(
            key_pipe3(&mount.mountpoint, "bavail", "value"),
            stat.f_bavail,
        );
        out.insert(
            key_pipe3(&mount.mountpoint, "files", "value"),
            stat.f_files,
        );
        out.insert(
            key_pipe3(&mount.mountpoint, "ffree", "value"),
            stat.f_ffree,
        );
    }

    out
}

#[cfg(test)]
mod slab_filesystem_tests {
    use super::{collect_filesystem_stats, parse_slabinfo_text};
    use crate::model::MountSnapshot;

    #[test]
    fn parse_slabinfo_text_reads_core_and_optional_slab_fields() {
        let text = "# comment\nkmalloc-64 1 2 64 8 1 : tunables 0 0 0 : slabdata 3 4 0\ninvalid\n";
        let parsed = parse_slabinfo_text(text);

        assert_eq!(parsed.get("kmalloc-64|active_objs|value"), Some(&1));
        assert_eq!(parsed.get("kmalloc-64|num_objs|value"), Some(&2));
        assert_eq!(parsed.get("kmalloc-64|objsize|bytes"), Some(&64));
        assert_eq!(parsed.get("kmalloc-64|objperslab|value"), Some(&8));
        assert_eq!(parsed.get("kmalloc-64|pagesperslab|value"), Some(&1));
        assert_eq!(parsed.get("kmalloc-64|active_slabs|value"), Some(&0));
        assert_eq!(parsed.get("kmalloc-64|num_slabs|value"), Some(&0));
    }

    #[test]
    fn collect_filesystem_stats_covers_success_and_invalid_mountpoint_paths() {
        let mounts = vec![
            MountSnapshot {
                device: "rootfs".to_string(),
                mountpoint: "/".to_string(),
                fs_type: "ext4".to_string(),
                read_only: false,
            },
            MountSnapshot {
                device: "none".to_string(),
                mountpoint: "\0invalid".to_string(),
                fs_type: "tmpfs".to_string(),
                read_only: false,
            },
        ];

        let out = collect_filesystem_stats(&mounts);
        assert!(out.contains_key("/|total_bytes|value"));
        assert!(out.contains_key("/|free_bytes|value"));
        assert!(out.contains_key("/|files|value"));
        assert!(!out.contains_key("\0invalid|total_bytes|value"));
    }
}
