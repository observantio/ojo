fn disk_queue_dir(name: &str) -> Option<PathBuf> {
    let class_queue = Path::new("/sys/class/block").join(name).join("queue");
    if class_queue.exists() {
        return Some(class_queue);
    }

    let block_queue = Path::new("/sys/block").join(name).join("queue");
    if block_queue.exists() {
        return Some(block_queue);
    }

    None
}

fn collect_disks(cache: &mut ReadCache) -> Result<Vec<DiskSnapshot>> {
    let stats = procfs::DiskStats::current()?;
    Ok(stats
        .0
        .into_iter()
        .map(|d| {
            let base = disk_queue_dir(&d.name);
            let logical_block_size = base
                .as_ref()
                .and_then(|p| cache.read_u64(p.join("logical_block_size")))
                .or_else(|| {
                    base.as_ref()
                        .and_then(|p| cache.read_u64(p.join("hw_sector_size")))
                })
                .or(Some(512));

            DiskSnapshot {
                name: d.name,
                has_counters: true,
                reads: d.reads,
                writes: d.writes,
                sectors_read: d.sectors_read,
                sectors_written: d.sectors_written,
                time_reading_ms: d.time_reading,
                time_writing_ms: d.time_writing,
                in_progress: d.in_progress,
                time_in_progress_ms: d.time_in_progress,
                weighted_time_in_progress_ms: d.weighted_time_in_progress,
                logical_block_size,
                physical_block_size: base
                    .as_ref()
                    .and_then(|p| cache.read_u64(p.join("physical_block_size"))),
                rotational: base
                    .as_ref()
                    .and_then(|p| cache.read_bool_num(p.join("rotational"))),
            }
        })
        .collect())
}

fn collect_net(cache: &mut ReadCache) -> Result<Vec<NetDevSnapshot>> {
    let devs = fs::read_to_string("/proc/net/dev")?;
    let mut out = Vec::new();
    let primary = read_primary_interfaces();
    let include_virtual = include_virtual_interfaces();

    for line in devs.lines().skip(2) {
        let mut parts = line.split(':');
        let name = parts.next().unwrap_or("").trim().to_string();
        let data = parts
            .next()
            .unwrap_or("")
            .split_whitespace()
            .collect::<Vec<_>>();

        if data.len() < 16 || name.is_empty() {
            continue;
        }

        let sys = Path::new("/sys/class/net").join(&name);
        let is_loopback = name == "lo";
        let is_virtual = Path::new("/sys/devices/virtual/net").join(&name).exists();
        let is_physical = !is_loopback && !is_virtual;
        let is_primary = primary.contains(&name);

        if !include_virtual && is_noise_interface(&name) && !is_primary {
            continue;
        }

        let stable_id = cache
            .read_trimmed(sys.join("address"))
            .filter(|mac| *mac != "00:00:00:00:00:00")
            .map(|mac| format!("mac:{mac}"))
            .or_else(|| {
                cache
                    .read_trimmed(sys.join("ifindex"))
                    .map(|v| format!("ifindex:{v}"))
            });

        let speed_mbps = cache.read_u64(sys.join("speed")).and_then(|v| {
            if v == 0 || v == u64::MAX || v == u32::MAX as u64 {
                None
            } else {
                Some(v)
            }
        });

        out.push(NetDevSnapshot {
            name,
            stable_id,
            interface_index: cache.read_u64(sys.join("ifindex")).map(|v| v as u32),
            interface_luid: None,
            is_virtual: Some(is_virtual),
            is_loopback: Some(is_loopback),
            is_physical: Some(is_physical),
            is_primary: Some(is_primary),
            mtu: cache.read_u64(sys.join("mtu")),
            speed_mbps,
            tx_queue_len: cache.read_u64(sys.join("tx_queue_len")),
            carrier_up: cache.read_bool_num(sys.join("carrier")),
            rx_bytes: data[0].parse().unwrap_or(0),
            rx_packets: data[1].parse().unwrap_or(0),
            rx_errs: data[2].parse().unwrap_or(0),
            rx_drop: data[3].parse().unwrap_or(0),
            rx_fifo: data[4].parse().unwrap_or(0),
            rx_frame: data[5].parse().unwrap_or(0),
            rx_compressed: data[6].parse().unwrap_or(0),
            rx_multicast: data[7].parse().unwrap_or(0),
            tx_bytes: data[8].parse().unwrap_or(0),
            tx_packets: data[9].parse().unwrap_or(0),
            tx_errs: data[10].parse().unwrap_or(0),
            tx_drop: data[11].parse().unwrap_or(0),
            tx_fifo: data[12].parse().unwrap_or(0),
            tx_colls: data[13].parse().unwrap_or(0),
            tx_carrier: data[14].parse().unwrap_or(0),
            tx_compressed: data[15].parse().unwrap_or(0),
        });
    }

    Ok(out)
}
