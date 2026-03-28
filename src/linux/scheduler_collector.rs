fn collect_netstat() -> Result<BTreeMap<String, u64>> {
    let Ok(contents) = fs::read_to_string("/proc/net/netstat") else {
        return Ok(BTreeMap::new());
    };
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

fn distribute_runnable(
    cpu_count: usize,
    runnable: f64,
    waiting_ns_by_cpu: &BTreeMap<usize, u64>,
) -> BTreeMap<String, f64> {
    let mut runqueue_depth = BTreeMap::new();
    let cpu_count = cpu_count.max(1);
    let wait_sum = waiting_ns_by_cpu.values().copied().sum::<u64>() as f64;

    runqueue_depth.insert("global_estimated_runnable".to_string(), runnable.max(0.0));

    for cpu in 0..cpu_count {
        let waiting_ns = waiting_ns_by_cpu.get(&cpu).copied().unwrap_or(0);
        let weight = if wait_sum > 0.0 {
            waiting_ns as f64 / wait_sum
        } else {
            1.0 / cpu_count as f64
        };
        runqueue_depth.insert(key_pipe2("cpu", cpu), (runnable * weight).max(0.0));
    }

    runqueue_depth
}

fn collect_schedstat_and_runqueue(
    cpu_count: usize,
    runnable: f64,
) -> Result<(BTreeMap<String, u64>, BTreeMap<String, f64>)> {
    let Ok(contents) = fs::read_to_string("/proc/schedstat") else {
        return Ok((
            BTreeMap::new(),
            distribute_runnable(cpu_count, runnable, &BTreeMap::new()),
        ));
    };
    let mut out = BTreeMap::new();
    let mut waiting_ns_by_cpu: BTreeMap<usize, u64> = BTreeMap::new();
    let mut version: Option<u64> = None;

    for line in contents.lines() {
        let cols = line.split_whitespace().collect::<Vec<_>>();
        if cols.len() >= 2 && cols[0] == "version" {
            version = cols[1].parse::<u64>().ok();
            if let Some(v) = version {
                out.insert("version|value".to_string(), v);
            }
            break;
        }
    }

    if let Some(v) = version {
        if v < 15 {
            return Ok((
                out,
                distribute_runnable(cpu_count, runnable, &BTreeMap::new()),
            ));
        }
    }

    for line in contents.lines() {
        let cols = line.split_whitespace().collect::<Vec<_>>();
        if cols.len() >= 2 && cols[0] == "version" {
            continue;
        }
        if cols.len() < 4 {
            continue;
        }

        let cpu = if let Some(raw) = cols[0].strip_prefix("cpu") {
            if raw.is_empty() {
                continue;
            }
            raw.parse::<usize>().ok()
        } else {
            None
        };

        let Some(cpu) = cpu else { continue };

        let n = cols.len();
        let running_ns = cols[n - 3].parse::<u64>().unwrap_or(0);
        let waiting_ns = cols[n - 2].parse::<u64>().unwrap_or(0);
        let timeslices = cols[n - 1].parse::<u64>().unwrap_or(0);

        out.insert(key_pipe3("cpu", "running_ns", cpu), running_ns);
        out.insert(key_pipe3("cpu", "waiting_ns", cpu), waiting_ns);
        out.insert(key_pipe3("cpu", "timeslices", cpu), timeslices);
        waiting_ns_by_cpu.insert(cpu, waiting_ns);
    }

    let runqueue_depth = distribute_runnable(cpu_count, runnable, &waiting_ns_by_cpu);

    Ok((out, runqueue_depth))
}

#[cfg(test)]
mod scheduler_tests {
    use super::{collect_netstat, collect_schedstat_and_runqueue, distribute_runnable};
    use std::collections::BTreeMap;

    #[test]
    fn collect_netstat_and_schedstat_smoke() {
        let net = collect_netstat().expect("collect netstat");
        let _ = net.len();

        let (sched, runqueue) = collect_schedstat_and_runqueue(4, 2.0).expect("collect schedstat");
        let _ = sched.len();
        assert!(runqueue.contains_key("global_estimated_runnable"));
    }

    #[test]
    fn distribute_runnable_covers_weighted_and_even_paths() {
        let mut weighted = BTreeMap::new();
        weighted.insert(0usize, 10u64);
        weighted.insert(1usize, 30u64);
        let by_wait = distribute_runnable(2, 4.0, &weighted);
        assert_eq!(by_wait.get("global_estimated_runnable"), Some(&4.0));
        assert!(by_wait.get("cpu|0").copied().unwrap_or_default() < 2.0);
        assert!(by_wait.get("cpu|1").copied().unwrap_or_default() > 2.0);

        let even = distribute_runnable(2, -1.0, &BTreeMap::new());
        assert_eq!(even.get("global_estimated_runnable"), Some(&0.0));
        assert_eq!(even.get("cpu|0"), Some(&0.0));
        assert_eq!(even.get("cpu|1"), Some(&0.0));
    }
}
