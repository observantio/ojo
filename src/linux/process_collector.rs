fn count_dir_entries(path: &Path) -> Option<u64> {
    let mut count = 0u64;
    let dir = fs::read_dir(path).ok()?;
    for entry in dir {
        if entry.is_ok() {
            count += 1;
        }
    }
    Some(count)
}

fn estimate_process_count_from_proc() -> Option<usize> {
    let entries = fs::read_dir("/proc").ok()?;
    let mut count = 0usize;
    for entry in entries.flatten() {
        let name = entry.file_name();
        if name
            .to_str()
            .map(|v| v.as_bytes().iter().all(|b| b.is_ascii_digit()))
            .unwrap_or(false)
        {
            count += 1;
        }
    }
    Some(count)
}

fn should_scan_fd_counts(process_count: usize) -> bool {
    if let Ok(raw) = std::env::var("OJO_PROCESS_FD_SCAN") {
        let normalized = raw.trim().to_ascii_lowercase();
        if matches!(normalized.as_str(), "0" | "false" | "no" | "off") {
            return false;
        }
        if matches!(normalized.as_str(), "1" | "true" | "yes" | "on") {
            return true;
        }
    }

    process_count <= PROCESS_FD_SCAN_LIMIT
}

fn collect_processes(
    cache: &mut ReadCache,
) -> Result<(Vec<ProcessSnapshot>, ProcessCollectionMeta)> {
    let mut out = Vec::new();
    let page_size = page_size_bytes();
    let process_count_hint = estimate_process_count_from_proc().unwrap_or(0);
    let fd_scan_enabled = should_scan_fd_counts(process_count_hint);

    for entry in all_processes()? {
        let Ok(process) = entry else { continue };
        let Ok(stat) = process.stat() else { continue };

        let io = process.io().ok();
        let proc_dir = Path::new("/proc").join(stat.pid.to_string());
        let status_path = proc_dir.join("status");
        let status_fields = cache
            .read_raw(&status_path)
            .map(parse_process_status_fields)
            .unwrap_or_default();

        out.push(ProcessSnapshot {
            pid: stat.pid,
            ppid: stat.ppid,
            comm: stat.comm,
            state: stat.state.to_string(),
            num_threads: stat.num_threads,
            priority: stat.priority,
            nice: stat.nice,
            minflt: stat.minflt,
            majflt: stat.majflt,
            vsize_bytes: stat.vsize,
            rss_pages: u64_to_i64(stat.rss),
            virtual_size_bytes: Some(stat.vsize),
            resident_bytes: status_fields
                .vm_rss_kib
                .map(|kib| kib.saturating_mul(1024))
                .or_else(|| Some(stat.rss.saturating_mul(page_size))),
            utime_ticks: stat.utime,
            stime_ticks: stat.stime,
            start_time_ticks: stat.starttime,
            processor: stat.processor.map(|value| value as i64),
            rt_priority: stat.rt_priority.map(|value| value as u64),
            policy: stat.policy.map(|value| value as u64),
            oom_score: cache.read_i64_first(proc_dir.join("oom_score")),
            fd_count: if fd_scan_enabled {
                count_dir_entries(&proc_dir.join("fd"))
            } else {
                None
            },
            fd_table_size: status_fields.fd_table_size,
            read_chars: io.as_ref().map(|v| v.rchar),
            write_chars: io.as_ref().map(|v| v.wchar),
            syscr: io.as_ref().map(|v| v.syscr),
            syscw: io.as_ref().map(|v| v.syscw),
            read_bytes: io.as_ref().map(|v| v.read_bytes),
            write_bytes: io.as_ref().map(|v| v.write_bytes),
            cancelled_write_bytes: io.as_ref().map(|v| u64_to_i64(v.cancelled_write_bytes)),
            vm_size_kib: status_fields.vm_size_kib,
            vm_rss_kib: status_fields.vm_rss_kib,
            vm_data_kib: status_fields.vm_data_kib,
            vm_stack_kib: status_fields.vm_stack_kib,
            vm_exe_kib: status_fields.vm_exe_kib,
            vm_lib_kib: status_fields.vm_lib_kib,
            vm_swap_kib: status_fields.vm_swap_kib,
            vm_pte_kib: status_fields.vm_pte_kib,
            vm_hwm_kib: status_fields.vm_hwm_kib,
            working_set_bytes: None,
            private_bytes: None,
            peak_working_set_bytes: None,
            pagefile_usage_bytes: None,
            commit_charge_bytes: None,
            voluntary_ctxt_switches: status_fields.voluntary_ctxt_switches,
            nonvoluntary_ctxt_switches: status_fields.nonvoluntary_ctxt_switches,
        });
    }

    Ok((out, ProcessCollectionMeta { fd_scan_enabled }))
}

#[cfg(test)]
mod process_collector_tests {
    use super::{collect_processes, estimate_process_count_from_proc, should_scan_fd_counts, ReadCache};

    #[test]
    fn process_collector_smoke_and_scan_toggle_paths() {
        std::env::remove_var("OJO_PROCESS_FD_SCAN");
        let hint = estimate_process_count_from_proc().unwrap_or(0);
        let _ = should_scan_fd_counts(hint);

        std::env::set_var("OJO_PROCESS_FD_SCAN", "0");
        assert!(!should_scan_fd_counts(0));
        std::env::set_var("OJO_PROCESS_FD_SCAN", "1");
        assert!(should_scan_fd_counts(usize::MAX));
        std::env::remove_var("OJO_PROCESS_FD_SCAN");

        let (procs, meta) = collect_processes(&mut ReadCache::default()).expect("collect processes");
        let _ = procs.len();
        let _ = meta.fd_scan_enabled;
    }
}
