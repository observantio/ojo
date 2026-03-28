fn collect_net_snmp() -> Result<BTreeMap<String, u64>> {
    let contents = fs::read_to_string("/proc/net/snmp")?;
    let mut out = BTreeMap::new();
    let mut pending: Option<(String, Vec<String>)> = None;

    for line in contents.lines() {
        let mut parts = line.split_whitespace();
        let Some(raw_prefix) = parts.next() else {
            continue;
        };
        let prefix = raw_prefix.trim_end_matches(':').to_string();
        let cols = parts.map(str::to_string).collect::<Vec<_>>();

        if let Some((pending_prefix, headers)) = pending.take() {
            if pending_prefix == prefix {
                for (header, value) in headers.iter().zip(cols.iter()) {
                    if let Ok(parsed) = value.parse::<u64>() {
                        out.insert(key_dot2(&prefix, header), parsed);
                    }
                }
            } else {
                pending = Some((prefix, cols));
            }
        } else {
            pending = Some((prefix, cols));
        }
    }

    Ok(out)
}

fn collect_sockets() -> Result<BTreeMap<String, u64>> {
    fn parse_sockstat(path: &str, family: &str, out: &mut BTreeMap<String, u64>) -> Result<()> {
        let contents = fs::read_to_string(path)?;
        for line in contents.lines() {
            let mut parts = line.split_whitespace();
            let Some(proto_raw) = parts.next() else {
                continue;
            };
            let proto = proto_raw.trim_end_matches(':').to_ascii_lowercase();
            let cols = parts.collect::<Vec<_>>();
            let mut i = 0usize;

            while i + 1 < cols.len() {
                let key = cols[i].to_ascii_lowercase();
                if let Ok(value) = cols[i + 1].parse::<u64>() {
                    out.insert(key_dot3(family, &proto, &key), value);
                }
                i += 2;
            }
        }
        Ok(())
    }

    let mut out = BTreeMap::new();
    parse_sockstat("/proc/net/sockstat", "v4", &mut out)?;
    if Path::new("/proc/net/sockstat6").exists() {
        parse_sockstat("/proc/net/sockstat6", "v6", &mut out)?;
    }
    Ok(out)
}

fn collect_interrupts() -> Result<BTreeMap<String, u64>> {
    let contents = fs::read_to_string("/proc/interrupts")?;
    let mut out = BTreeMap::new();
    let mut cpus = 0usize;

    for (idx, line) in contents.lines().enumerate() {
        if idx == 0 {
            cpus = line
                .split_whitespace()
                .filter(|part| part.starts_with("CPU"))
                .count();
            continue;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let Some((irq_raw, rest)) = trimmed.split_once(':') else {
            continue;
        };
        let irq = irq_raw.trim();
        let cols = rest.split_whitespace().collect::<Vec<_>>();

        for (cpu, value) in cols.iter().take(cpus.min(cols.len())).enumerate() {
            if let Ok(value) = value.parse::<u64>() {
                out.insert(key_pipe2(irq, cpu), value);
            }
        }
    }

    Ok(out)
}

fn collect_softirqs() -> Result<BTreeMap<String, u64>> {
    let contents = fs::read_to_string("/proc/softirqs")?;
    let mut out = BTreeMap::new();
    let mut cpus = 0usize;

    for (idx, line) in contents.lines().enumerate() {
        if idx == 0 {
            cpus = line
                .split_whitespace()
                .filter(|part| part.starts_with("CPU"))
                .count();
            continue;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let Some((kind_raw, rest)) = trimmed.split_once(':') else {
            continue;
        };
        let kind = kind_raw.trim();
        let cols = rest.split_whitespace().collect::<Vec<_>>();

        for (cpu, value) in cols.iter().take(cpus.min(cols.len())).enumerate() {
            if let Ok(value) = value.parse::<u64>() {
                out.insert(key_pipe2(kind, cpu), value);
            }
        }
    }

    Ok(out)
}

fn collect_softnet() -> Result<Vec<SoftnetCpuSnapshot>> {
    let contents = fs::read_to_string("/proc/net/softnet_stat")?;
    Ok(contents
        .lines()
        .enumerate()
        .filter_map(|(cpu, line)| {
            let cols = line.split_whitespace().collect::<Vec<_>>();
            if cols.len() < 3 {
                return None;
            }

            Some(SoftnetCpuSnapshot {
                cpu,
                processed: u64::from_str_radix(cols[0], 16).unwrap_or(0),
                dropped: u64::from_str_radix(cols[1], 16).unwrap_or(0),
                time_squeezed: u64::from_str_radix(cols[2], 16).unwrap_or(0),
                cpu_collision: cols.get(8).and_then(|v| u64::from_str_radix(v, 16).ok()),
                received_rps: cols.get(9).and_then(|v| u64::from_str_radix(v, 16).ok()),
                flow_limit_count: cols.get(10).and_then(|v| u64::from_str_radix(v, 16).ok()),
            })
        })
        .collect())
}

#[cfg(test)]
mod kernel_net_tests {
    use super::{
        collect_interrupts, collect_net_snmp, collect_sockets, collect_softirqs, collect_softnet,
    };

    #[test]
    fn kernel_net_collectors_smoke() {
        let _ = collect_net_snmp().expect("collect net snmp");
        let _ = collect_sockets().expect("collect sockets");
        let _ = collect_interrupts().expect("collect interrupts");
        let _ = collect_softirqs().expect("collect softirqs");
        let _ = collect_softnet().expect("collect softnet");
    }
}
