fn collect_cgroup() -> Result<(BTreeMap<String, u64>, CgroupMode)> {
    let root = Path::new("/sys/fs/cgroup");
    let mounts = fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
    let mut v1_mounts = Vec::new();
    let mut v2_mounts = Vec::new();

    for line in mounts.lines() {
        let Some((left, right)) = line.split_once(" - ") else {
            continue;
        };
        let left_cols = left.split_whitespace().collect::<Vec<_>>();
        let right_cols = right.split_whitespace().collect::<Vec<_>>();
        if left_cols.len() < 5 || right_cols.is_empty() {
            continue;
        }
        let mountpoint = PathBuf::from(unescape_mount_field(left_cols[4]));
        match right_cols[0] {
            "cgroup2" => v2_mounts.push(mountpoint),
            "cgroup" => v1_mounts.push(mountpoint),
            _ => {}
        }
    }

    if !root.exists() && v1_mounts.is_empty() && v2_mounts.is_empty() {
        return Ok((BTreeMap::new(), CgroupMode::None));
    }

    let mut out = BTreeMap::new();
    let has_v2 = root.join("cgroup.controllers").exists() || !v2_mounts.is_empty();
    let has_v1 = !v1_mounts.is_empty()
        || fs::read_dir(root)
            .ok()
            .map(|entries| {
                entries.flatten().any(|entry| {
                    let path = entry.path();
                    path.is_dir()
                        && path
                            .file_name()
                            .and_then(|v| v.to_str())
                            .map(|name| {
                                matches!(name, "cpu" | "cpuacct" | "memory" | "blkio" | "pids")
                            })
                            .unwrap_or(false)
                })
            })
            .unwrap_or(false);

    if has_v2 {
        if v2_mounts.is_empty() {
            collect_cgroup_v2_tree(root, &mut out);
        } else {
            for mount in v2_mounts {
                collect_cgroup_v2_tree(&mount, &mut out);
            }
        }
    }
    if has_v1 {
        if v1_mounts.is_empty() {
            collect_cgroup_v1_tree(root, "unknown", &mut out);
        } else {
            for mount in v1_mounts {
                let controller = mount
                    .file_name()
                    .and_then(|v| v.to_str())
                    .unwrap_or("unknown");
                collect_cgroup_v1_tree(&mount, controller, &mut out);
            }
        }
    }

    let mode = match (has_v1, has_v2) {
        (true, true) => CgroupMode::Hybrid,
        (true, false) => CgroupMode::V1,
        (false, true) => CgroupMode::V2,
        (false, false) => CgroupMode::None,
    };

    Ok((out, mode))
}

fn collect_cgroup_v2_tree(root: &Path, out: &mut BTreeMap<String, u64>) {
    let mut stack = vec![(root.to_path_buf(), 0usize)];
    let mut visited = 0usize;

    while let Some((dir, depth)) = stack.pop() {
        visited += 1;
        if visited > CGROUP_MAX_DIRS {
            break;
        }
        if depth > CGROUP_MAX_DEPTH {
            continue;
        }

        let rel_raw = dir
            .strip_prefix(root)
            .ok()
            .and_then(|p| p.to_str())
            .filter(|p| !p.is_empty())
            .map(|p| format!("v2/{p}"))
            .unwrap_or_else(|| "v2/root".to_string());
        let rel = normalize_cgroup_scope(&rel_raw);

        collect_cgroup_v2_dir(&dir, &rel, out);

        if let Ok(entries) = fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push((path, depth + 1));
                }
            }
        }
    }
}

fn collect_cgroup_v1_tree(root: &Path, controller_raw: &str, out: &mut BTreeMap<String, u64>) {
    let controller = controller_raw.replace(',', "+");
    let mut stack = vec![(root.to_path_buf(), 0usize)];
    let mut visited = 0usize;
    while let Some((dir, depth)) = stack.pop() {
        visited += 1;
        if visited > CGROUP_MAX_DIRS {
            break;
        }
        if depth > CGROUP_MAX_DEPTH {
            continue;
        }

        let rel_raw = dir
            .strip_prefix(root)
            .ok()
            .and_then(|p| p.to_str())
            .filter(|p| !p.is_empty())
            .map(|p| format!("v1/{controller}/{p}"))
            .unwrap_or_else(|| format!("v1/{controller}/root"));
        let rel = normalize_cgroup_scope(&rel_raw);

        collect_cgroup_v1_dir(&dir, &rel, out);

        if let Ok(children) = fs::read_dir(&dir) {
            for child in children.flatten() {
                let path = child.path();
                if path.is_dir() {
                    stack.push((path, depth + 1));
                }
            }
        }
    }
}

fn collect_cgroup_v2_dir(path: &Path, scope: &str, out: &mut BTreeMap<String, u64>) {
    for file in [
        "memory.current",
        "memory.swap.current",
        "memory.swap.max",
        "pids.current",
        "pids.max",
        "cpu.weight",
    ] {
        let file_path = path.join(file);
        let text = fs::read_to_string(&file_path).ok();
        let (parsed, is_max) = text
            .as_deref()
            .map(|v| parse_u64_with_max_flag(v.trim()))
            .unwrap_or((None, false));
        if let Some(value) = parsed {
            out.insert(key_pipe3(scope, file, "value"), value);
        }
        if is_max {
            out.insert(key_pipe3(scope, file, "is_max"), 1);
        }
    }

    if let Ok(cpu_max) = fs::read_to_string(path.join("cpu.max")) {
        let cols = cpu_max.split_whitespace().collect::<Vec<_>>();
        if cols.len() >= 2 {
            let (quota, quota_is_max) = parse_u64_with_max_flag(cols[0]);
            if let Some(value) = quota {
                out.insert(key_pipe3(scope, "cpu.max.quota", "value"), value);
            }
            if quota_is_max {
                out.insert(key_pipe3(scope, "cpu.max.quota", "is_max"), 1);
            }

            let (period, period_is_max) = parse_u64_with_max_flag(cols[1]);
            if let Some(value) = period {
                out.insert(key_pipe3(scope, "cpu.max.period", "value"), value);
            }
            if period_is_max {
                out.insert(key_pipe3(scope, "cpu.max.period", "is_max"), 1);
            }
        }
    }

    if let Ok(cpu_stat) = fs::read_to_string(path.join("cpu.stat")) {
        for line in cpu_stat.lines() {
            let cols = line.split_whitespace().collect::<Vec<_>>();
            if cols.len() != 2 {
                continue;
            }
            let (parsed, is_max) = parse_u64_with_max_flag(cols[1]);
            if let Some(value) = parsed {
                out.insert(key_pipe3(scope, "cpu.stat", cols[0]), value);
            }
            if is_max {
                out.insert(key_pipe4(scope, "cpu.stat", cols[0], "is_max"), 1);
            }
        }
    }

    if let Ok(memory_stat) = fs::read_to_string(path.join("memory.stat")) {
        for line in memory_stat.lines() {
            let cols = line.split_whitespace().collect::<Vec<_>>();
            if cols.len() != 2 {
                continue;
            }
            let (parsed, is_max) = parse_u64_with_max_flag(cols[1]);
            if let Some(value) = parsed {
                out.insert(key_pipe3(scope, "memory.stat", cols[0]), value);
            }
            if is_max {
                out.insert(key_pipe4(scope, "memory.stat", cols[0], "is_max"), 1);
            }
        }
    }

    if let Ok(io_stat) = fs::read_to_string(path.join("io.stat")) {
        for line in io_stat.lines() {
            let cols = line.split_whitespace().collect::<Vec<_>>();
            if cols.len() < 2 {
                continue;
            }
            let dev = cols[0];
            for kv in &cols[1..] {
                let Some((k, v)) = kv.split_once('=') else {
                    continue;
                };
                let (parsed, is_max) = parse_u64_with_max_flag(v);
                if let Some(value) = parsed {
                    out.insert(key_pipe4(scope, "io.stat", dev, k), value);
                }
                if is_max {
                    out.insert(key_pipe4(scope, "io.stat", dev, format!("{k}.is_max")), 1);
                }
            }
        }
    }
}

fn collect_cgroup_v1_dir(path: &Path, scope: &str, out: &mut BTreeMap<String, u64>) {
    for file in [
        "memory.usage_in_bytes",
        "memory.limit_in_bytes",
        "memory.memsw.usage_in_bytes",
        "memory.memsw.limit_in_bytes",
        "memory.kmem.usage_in_bytes",
        "pids.current",
        "pids.max",
        "cpu.shares",
        "cpu.cfs_quota_us",
        "cpu.cfs_period_us",
        "cpuacct.usage",
    ] {
        let file_path = path.join(file);
        let text = fs::read_to_string(&file_path).ok();
        let (parsed, is_max) = text
            .as_deref()
            .map(|v| parse_u64_with_max_flag(v.trim()))
            .unwrap_or((None, false));
        if let Some(value) = parsed {
            out.insert(key_pipe3(scope, file, "value"), value);
        }
        if is_max {
            out.insert(key_pipe3(scope, file, "is_max"), 1);
        }
    }

    if let Ok(contents) = fs::read_to_string(path.join("blkio.throttle.io_service_bytes")) {
        for line in contents.lines() {
            let cols = line.split_whitespace().collect::<Vec<_>>();
            if cols.len() < 3 {
                continue;
            }
            let dev = cols[0];
            let op = cols[1].to_ascii_lowercase();
            let (parsed, is_max) = parse_u64_with_max_flag(cols[2]);
            if let Some(value) = parsed {
                out.insert(
                    key_pipe4(scope, "blkio.throttle.io_service_bytes", dev, op.as_str()),
                    value,
                );
            }
            if is_max {
                out.insert(
                    key_pipe4(
                        scope,
                        "blkio.throttle.io_service_bytes",
                        dev,
                        format!("{op}.is_max"),
                    ),
                    1,
                );
            }
        }
    }
}
