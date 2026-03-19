fn collect_processes(page_size: u64, ticks_per_second: u64) -> Result<Vec<ProcessSnapshot>> {
    let Some(output) = run_command_optional(
        "ps",
        &[
            "-e", "-o", "pid=", "-o", "ppid=", "-o", "s=", "-o", "nlwp=", "-o", "pri=", "-o",
            "nice=", "-o", "rss=", "-o", "vsz=", "-o", "time=", "-o", "fname=",
        ],
    ) else {
        return Ok(Vec::new());
    };

    let mut out = Vec::new();

    for line in output.lines() {
        let cols = line.split_whitespace().collect::<Vec<_>>();
        if cols.len() < 10 {
            continue;
        }

        let pid = cols[0].parse::<i32>().unwrap_or(0);
        let ppid = cols[1].parse::<i32>().unwrap_or(0);
        let state = cols[2].to_string();
        let num_threads = cols[3].parse::<i64>().unwrap_or(0);
        let priority = cols[4].parse::<i64>().unwrap_or(0);
        let nice = cols[5].parse::<i64>().unwrap_or(0);
        let rss_kib = cols[6].parse::<u64>().unwrap_or(0);
        let vsz_kib = cols[7].parse::<u64>().unwrap_or(0);
        let cpu_ticks = parse_cpu_time_to_ticks(cols[8], ticks_per_second);
        let comm = cols[9].to_string();

        let resident_bytes = rss_kib.saturating_mul(1024);
        let vsize_bytes = vsz_kib.saturating_mul(1024);

        out.push(ProcessSnapshot {
            pid,
            ppid,
            comm,
            state,
            num_threads,
            priority,
            nice,
            minflt: 0,
            majflt: 0,
            vsize_bytes,
            rss_pages: u64_to_i64(resident_bytes / page_size.max(1)),
            virtual_size_bytes: Some(vsize_bytes),
            resident_bytes: Some(resident_bytes),
            utime_ticks: cpu_ticks,
            stime_ticks: 0,
            start_time_ticks: 0,
            processor: None,
            rt_priority: None,
            policy: None,
            oom_score: None,
            fd_count: count_fds(pid),
            fd_table_size: None,
            read_chars: None,
            write_chars: None,
            syscr: None,
            syscw: None,
            read_bytes: None,
            write_bytes: None,
            cancelled_write_bytes: None,
            vm_size_kib: Some(vsz_kib),
            vm_rss_kib: Some(rss_kib),
            vm_data_kib: None,
            vm_stack_kib: None,
            vm_exe_kib: None,
            vm_lib_kib: None,
            vm_swap_kib: None,
            vm_pte_kib: None,
            vm_hwm_kib: None,
            working_set_bytes: None,
            private_bytes: None,
            peak_working_set_bytes: None,
            pagefile_usage_bytes: None,
            commit_charge_bytes: None,
            voluntary_ctxt_switches: None,
            nonvoluntary_ctxt_switches: None,
        });
    }

    Ok(out)
}
