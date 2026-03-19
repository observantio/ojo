fn collect_swaps() -> Result<Vec<SwapDeviceSnapshot>> {
    let contents = fs::read_to_string("/proc/swaps")?;
    let mut out = Vec::new();

    for (idx, line) in contents.lines().enumerate() {
        if idx == 0 {
            continue;
        }

        let cols = line.split_whitespace().collect::<Vec<_>>();
        if cols.len() < 5 {
            continue;
        }

        let size_kib = cols[2].parse::<u64>().unwrap_or(0);
        let used_kib = cols[3].parse::<u64>().unwrap_or(0);
        let priority = cols[4].parse::<i64>().unwrap_or(0);

        out.push(SwapDeviceSnapshot {
            device: cols[0].to_string(),
            swap_type: cols[1].to_string(),
            size_bytes: size_kib.saturating_mul(1024),
            used_bytes: used_kib.saturating_mul(1024),
            priority,
        });
    }

    Ok(out)
}

fn collect_mounts() -> Result<Vec<MountSnapshot>> {
    let contents = fs::read_to_string("/proc/mounts")?;
    let mut out = Vec::new();

    for line in contents.lines() {
        let cols = line.split_whitespace().collect::<Vec<_>>();
        if cols.len() < 4 {
            continue;
        }

        let fs_type = cols[2].to_string();
        if !include_pseudo_filesystems() && is_pseudo_filesystem(&fs_type) {
            continue;
        }

        out.push(MountSnapshot {
            device: unescape_mount_field(cols[0]),
            mountpoint: unescape_mount_field(cols[1]),
            fs_type,
            read_only: cols[3].split(',').any(|option| option == "ro"),
        });
    }

    Ok(out)
}

fn collect_cpuinfo(cache: &mut ReadCache) -> Result<Vec<CpuInfoSnapshot>> {
    let contents = fs::read_to_string("/proc/cpuinfo")?;
    let mut out = Vec::new();
    let mut current = CpuInfoSnapshot::default();
    let mut seen = false;

    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            if seen {
                out.push(current);
                current = CpuInfoSnapshot::default();
                seen = false;
            }
            continue;
        }

        let Some((key, value)) = trimmed.split_once(':') else {
            continue;
        };
        let key = key.trim();
        let value = value.trim();
        seen = true;

        match key {
            "processor" => current.cpu = value.parse::<usize>().unwrap_or(0),
            "vendor_id" | "CPU implementer" | "Hardware" => {
                current.vendor_id = Some(value.to_string())
            }
            "model name" | "Processor" | "model" | "cpu" => {
                if current.model_name.is_none() {
                    current.model_name = Some(value.to_string());
                }
            }
            "cpu MHz" => current.mhz = value.parse::<f64>().ok(),
            "cache size" | "L2 cache" => {
                let size_kib = value
                    .split_whitespace()
                    .next()
                    .and_then(|v| v.parse::<u64>().ok());
                if current.cache_size_bytes.is_none() {
                    current.cache_size_bytes = size_kib.map(|v| v.saturating_mul(1024));
                }
            }
            _ => {}
        }
    }

    if seen {
        out.push(current);
    }

    for cpu in &mut out {
        if cpu.mhz.is_none() {
            cpu.mhz = read_cpu_frequency_mhz(cache, cpu.cpu);
        }
    }

    Ok(out)
}

fn collect_zoneinfo() -> Result<BTreeMap<String, u64>> {
    let contents = fs::read_to_string("/proc/zoneinfo")?;
    let mut out = BTreeMap::new();
    let mut current_node = String::new();
    let mut current_zone = String::new();

    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if let Some(rest) = trimmed.strip_prefix("Node ") {
            if let Some((node_part, zone_part)) = rest.split_once(", zone") {
                current_node = node_part.trim().to_string();
                current_zone = zone_part.trim().to_string();
            }
            continue;
        }

        if current_node.is_empty() || current_zone.is_empty() {
            continue;
        }

        let cols = trimmed.split_whitespace().collect::<Vec<_>>();
        if cols.len() != 2 {
            continue;
        }

        if let Ok(value) = cols[1].parse::<u64>() {
            out.insert(key_pipe3(&current_node, &current_zone, cols[0]), value);
        }
    }

    Ok(out)
}

fn collect_buddyinfo() -> Result<BTreeMap<String, u64>> {
    let contents = fs::read_to_string("/proc/buddyinfo")?;
    let mut out = BTreeMap::new();

    for line in contents.lines() {
        let cols = line.split_whitespace().collect::<Vec<_>>();
        if cols.len() < 5 || cols[0] != "Node" {
            continue;
        }

        let node = cols[1].trim_end_matches(',');
        let zone = cols[3].trim_end_matches(',');

        for (order, value) in cols[4..].iter().enumerate() {
            if let Ok(parsed) = value.parse::<u64>() {
                out.insert(key_pipe3(node, zone, order), parsed);
            }
        }
    }

    Ok(out)
}
