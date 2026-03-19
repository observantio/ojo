fn collect_swaps() -> Result<Vec<SwapDeviceSnapshot>> {
    let Some(output) = run_command_optional("swap", &["-l"]) else {
        return Ok(Vec::new());
    };

    let mut out = Vec::new();

    for (idx, line) in output.lines().enumerate() {
        if idx == 0 {
            continue;
        }

        let cols = line.split_whitespace().collect::<Vec<_>>();
        if cols.len() < 5 {
            continue;
        }

        let blocks = cols[3].parse::<u64>().unwrap_or(0);
        let free = cols[4].parse::<u64>().unwrap_or(0);
        let size_bytes = blocks.saturating_mul(512);
        let free_bytes = free.saturating_mul(512);

        out.push(SwapDeviceSnapshot {
            device: cols[0].to_string(),
            swap_type: "swap".to_string(),
            size_bytes,
            used_bytes: size_bytes.saturating_sub(free_bytes),
            priority: 0,
        });
    }

    Ok(out)
}

fn collect_mounts() -> Result<Vec<MountSnapshot>> {
    let contents = fs::read_to_string("/etc/mnttab")?;
    let mut out = Vec::new();

    for line in contents.lines() {
        let cols = line.split_whitespace().collect::<Vec<_>>();
        if cols.len() < 4 {
            continue;
        }

        let device = decode_mount_field(cols[0]);
        let mountpoint = decode_mount_field(cols[1]);
        let fs_type = cols[2].to_string();
        let read_only = cols[3].split(',').any(|v| v == "ro");

        out.push(MountSnapshot {
            device,
            mountpoint,
            fs_type,
            read_only,
        });
    }

    Ok(out)
}

fn collect_cpuinfo(kstats: &KstatMap) -> Result<Vec<CpuInfoSnapshot>> {
    let mut out: BTreeMap<usize, CpuInfoSnapshot> = BTreeMap::new();

    for (key, value) in kstats {
        let Some((module, instance, _name, stat)) = split_kstat_key(key) else {
            continue;
        };

        if module != "cpu_info" {
            continue;
        }

        let cpu = instance.parse::<usize>().unwrap_or(0);
        let entry = out.entry(cpu).or_insert_with(|| CpuInfoSnapshot {
            cpu,
            ..CpuInfoSnapshot::default()
        });

        match stat {
            "vendor_id" => entry.vendor_id = Some(value.clone()),
            "brand" => entry.model_name = Some(value.clone()),
            "implementation" if entry.model_name.is_none() => {
                entry.model_name = Some(value.clone())
            }
            "clock_MHz" => entry.mhz = value.parse::<f64>().ok(),
            "current_clock_Hz" if entry.mhz.is_none() => {
                entry.mhz = value.parse::<f64>().ok().map(|v| v / 1_000_000.0)
            }
            "cache_size" | "l2_cache_size" => {
                entry.cache_size_bytes = value.parse::<u64>().ok().map(|v| v.saturating_mul(1024))
            }
            _ => {}
        }
    }

    Ok(out.into_values().collect())
}

fn collect_disks(kstats: &KstatMap) -> Result<Vec<DiskSnapshot>> {
    let mut groups: HashMap<String, DiskAccum> = HashMap::new();

    for (key, value) in kstats {
        let Some((module, instance, name, stat)) = split_kstat_key(key) else {
            continue;
        };

        if name.contains(',') {
            continue;
        }

        let Some(parsed) = value.parse::<u64>().ok() else {
            continue;
        };

        let interesting = matches!(
            stat,
            "reads"
                | "writes"
                | "nread"
                | "nwritten"
                | "rtime"
                | "wtime"
                | "rlentime"
                | "wlentime"
                | "rcnt"
                | "wcnt"
        );

        if !interesting {
            continue;
        }

        let group_key = format!("{module}:{instance}:{name}");
        let disk = groups.entry(group_key).or_insert_with(|| DiskAccum {
            name: name.to_string(),
            ..DiskAccum::default()
        });

        disk.seen = true;

        match stat {
            "reads" => disk.reads = parsed,
            "writes" => disk.writes = parsed,
            "nread" => disk.nread = parsed,
            "nwritten" => disk.nwritten = parsed,
            "rtime" => disk.rtime_ns = parsed,
            "wtime" => disk.wtime_ns = parsed,
            "rlentime" => disk.rlentime_ns = parsed,
            "wlentime" => disk.wlentime_ns = parsed,
            "rcnt" => disk.rcnt = parsed,
            "wcnt" => disk.wcnt = parsed,
            _ => {}
        }
    }

    let mut out = groups
        .into_values()
        .filter(|d| d.seen && !d.name.is_empty())
        .map(|d| DiskSnapshot {
            name: d.name,
            has_counters: d.reads > 0 || d.writes > 0 || d.nread > 0 || d.nwritten > 0,
            reads: d.reads,
            writes: d.writes,
            sectors_read: d.nread / 512,
            sectors_written: d.nwritten / 512,
            time_reading_ms: d.rtime_ns / 1_000_000,
            time_writing_ms: d.wtime_ns / 1_000_000,
            in_progress: d.rcnt.saturating_add(d.wcnt),
            time_in_progress_ms: 0,
            weighted_time_in_progress_ms: d.rlentime_ns.saturating_add(d.wlentime_ns) / 1_000_000,
            logical_block_size: None,
            physical_block_size: None,
            rotational: None,
        })
        .collect::<Vec<_>>();

    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

fn collect_net(kstats: &KstatMap) -> Result<Vec<NetDevSnapshot>> {
    let mut groups: HashMap<String, NetAccum> = HashMap::new();

    for (key, value) in kstats {
        let Some((module, instance, name, stat)) = split_kstat_key(key) else {
            continue;
        };

        if name.contains(',') {
            continue;
        }

        let interesting = matches!(
            stat,
            "rbytes64"
                | "rbytes"
                | "obytes64"
                | "obytes"
                | "ipackets64"
                | "ipackets"
                | "opackets64"
                | "opackets"
                | "ierrors"
                | "oerrors"
                | "collisions"
                | "multircv"
                | "ifspeed"
                | "mtu"
                | "tx_queue_len"
                | "link_up"
        );

        if !interesting {
            continue;
        }

        let group_key = format!("{module}:{instance}:{name}");
        let net = groups.entry(group_key).or_insert_with(|| NetAccum {
            name: name.to_string(),
            ..NetAccum::default()
        });

        net.seen = true;

        match stat {
            "rbytes64" | "rbytes" => net.rx_bytes = value.parse::<u64>().unwrap_or(net.rx_bytes),
            "obytes64" | "obytes" => net.tx_bytes = value.parse::<u64>().unwrap_or(net.tx_bytes),
            "ipackets64" | "ipackets" => {
                net.rx_packets = value.parse::<u64>().unwrap_or(net.rx_packets)
            }
            "opackets64" | "opackets" => {
                net.tx_packets = value.parse::<u64>().unwrap_or(net.tx_packets)
            }
            "ierrors" => net.rx_errs = value.parse::<u64>().unwrap_or(0),
            "oerrors" => net.tx_errs = value.parse::<u64>().unwrap_or(0),
            "collisions" => net.collisions = value.parse::<u64>().unwrap_or(0),
            "multircv" => net.rx_multicast = value.parse::<u64>().unwrap_or(0),
            "ifspeed" => net.speed_bps = value.parse::<u64>().unwrap_or(0),
            "mtu" => net.mtu = value.parse::<u64>().ok(),
            "tx_queue_len" => net.tx_queue_len = value.parse::<u64>().ok(),
            "link_up" => {
                net.carrier_up = match value.trim() {
                    "0" => Some(false),
                    "1" => Some(true),
                    _ => None,
                }
            }
            _ => {}
        }
    }

    let mut out = groups
        .into_values()
        .filter(|n| n.seen && !n.name.is_empty())
        .map(|n| {
            let is_loopback = n.name == "lo0" || n.name == "lo";
            NetDevSnapshot {
                name: n.name.clone(),
                stable_id: Some(format!("name:{}", n.name)),
                interface_index: None,
                interface_luid: None,
                is_virtual: None,
                is_loopback: Some(is_loopback),
                is_physical: None,
                is_primary: None,
                mtu: n.mtu,
                speed_mbps: if n.speed_bps > 0 {
                    Some(n.speed_bps / 1_000_000)
                } else {
                    None
                },
                tx_queue_len: n.tx_queue_len,
                carrier_up: n.carrier_up,
                rx_bytes: n.rx_bytes,
                rx_packets: n.rx_packets,
                rx_errs: n.rx_errs,
                rx_drop: 0,
                rx_fifo: 0,
                rx_frame: 0,
                rx_compressed: 0,
                rx_multicast: n.rx_multicast,
                tx_bytes: n.tx_bytes,
                tx_packets: n.tx_packets,
                tx_errs: n.tx_errs,
                tx_drop: 0,
                tx_fifo: 0,
                tx_colls: n.collisions,
                tx_carrier: 0,
                tx_compressed: 0,
            }
        })
        .collect::<Vec<_>>();

    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

