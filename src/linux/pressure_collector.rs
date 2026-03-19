fn collect_pressure() -> Result<BTreeMap<String, f64>> {
    let mut out = BTreeMap::new();

    for resource in ["cpu", "memory", "io", "irq"] {
        let path = Path::new("/proc/pressure").join(resource);
        let Ok(contents) = fs::read_to_string(path) else {
            continue;
        };

        for line in contents.lines() {
            let mut parts = line.split_whitespace();
            let Some(scope) = parts.next() else { continue };

            for field in parts {
                let Some((name, value)) = field.split_once('=') else {
                    continue;
                };
                if name == "total" {
                    continue;
                }
                if let Ok(parsed) = value.parse::<f64>() {
                    out.insert(key_dot3(resource, scope, name), parsed / 100.0);
                }
            }
        }
    }

    Ok(out)
}

fn collect_pressure_totals() -> Result<BTreeMap<String, u64>> {
    let mut out = BTreeMap::new();

    for resource in ["cpu", "memory", "io", "irq"] {
        let path = Path::new("/proc/pressure").join(resource);
        let Ok(contents) = fs::read_to_string(path) else {
            continue;
        };

        for line in contents.lines() {
            let mut parts = line.split_whitespace();
            let Some(scope) = parts.next() else { continue };

            for field in parts {
                let Some((name, value)) = field.split_once('=') else {
                    continue;
                };
                if name != "total" {
                    continue;
                }
                if let Ok(parsed) = value.parse::<u64>() {
                    out.insert(key_dot2(resource, scope), parsed);
                }
            }
        }
    }

    Ok(out)
}

fn collect_proc_stat_totals() -> Result<(u64, u64)> {
    let contents = fs::read_to_string("/proc/stat")?;
    let mut interrupts_total = 0;
    let mut softirqs_total = 0;

    for line in contents.lines() {
        let mut parts = line.split_whitespace();
        match parts.next() {
            Some("intr") => {
                interrupts_total = parts.next().and_then(|v| v.parse().ok()).unwrap_or(0);
            }
            Some("softirq") => {
                softirqs_total = parts.next().and_then(|v| v.parse().ok()).unwrap_or(0);
            }
            _ => {}
        }
    }

    Ok((interrupts_total, softirqs_total))
}

fn collect_uptime_secs() -> Result<f64> {
    let value = fs::read_to_string("/proc/uptime")?;
    Ok(value
        .split_whitespace()
        .next()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.0))
}
