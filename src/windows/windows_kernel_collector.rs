fn collect_vmstat(perf: Option<&SystemPerformanceInformation>) -> BTreeMap<String, i64> {
    let mut out = BTreeMap::new();
    let Some(p) = perf else { return out };

    out.insert("pgfault".to_string(), p.page_fault_count as i64);
    out.insert("pgmajfault".to_string(), p.page_read_io_count as i64);
    out.insert("pgpgin".to_string(), p.page_read_count as i64);
    out.insert(
        "pgpgout".to_string(),
        (p.dirty_pages_write_count as i64).saturating_add(p.mapped_pages_write_count as i64),
    );

    out.insert(
        "windows.available_pages".to_string(),
        p.available_pages as i64,
    );
    out.insert(
        "windows.committed_pages".to_string(),
        p.committed_pages as i64,
    );
    out.insert(
        "windows.commit_limit_pages".to_string(),
        p.commit_limit as i64,
    );
    out.insert(
        "windows.peak_commitment_pages".to_string(),
        p.peak_commitment as i64,
    );
    out.insert(
        "windows.paged_pool_pages".to_string(),
        p.paged_pool_pages as i64,
    );
    out.insert(
        "windows.non_paged_pool_pages".to_string(),
        p.non_paged_pool_pages as i64,
    );
    out.insert(
        "windows.copy_on_write".to_string(),
        p.copy_on_write_count as i64,
    );
    out.insert(
        "windows.transition_faults".to_string(),
        p.transition_count as i64,
    );
    out.insert(
        "windows.cache_transition_faults".to_string(),
        p.cache_transition_count as i64,
    );
    out.insert(
        "windows.demand_zero_faults".to_string(),
        p.demand_zero_count as i64,
    );
    out.insert(
        "windows.page_read_ios".to_string(),
        p.page_read_io_count as i64,
    );
    out.insert(
        "windows.cache_read_ios".to_string(),
        p.cache_io_count as i64,
    );
    out.insert(
        "windows.mapped_write_ios".to_string(),
        p.mapped_write_io_count as i64,
    );
    out.insert(
        "windows.dirty_write_ios".to_string(),
        p.dirty_write_io_count as i64,
    );
    out.insert("windows.system_calls".to_string(), p.system_calls as i64);
    out.insert(
        "windows.context_switches".to_string(),
        p.context_switches as i64,
    );

    out
}

fn collect_net_snmp() -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();
    unsafe {
        let mut ip = MIB_IPSTATS_LH::default();
        if GetIpStatistics(&mut ip) == 0 {
            out.insert("Ip.InReceives".to_string(), ip.dwInReceives as u64);
            out.insert("Ip.InHdrErrors".to_string(), ip.dwInHdrErrors as u64);
            out.insert("Ip.InAddrErrors".to_string(), ip.dwInAddrErrors as u64);
            out.insert("Ip.ForwDatagrams".to_string(), ip.dwForwDatagrams as u64);
            out.insert(
                "Ip.InUnknownProtos".to_string(),
                ip.dwInUnknownProtos as u64,
            );
            out.insert("Ip.InDiscards".to_string(), ip.dwInDiscards as u64);
            out.insert("Ip.InDelivers".to_string(), ip.dwInDelivers as u64);
            out.insert("Ip.OutRequests".to_string(), ip.dwOutRequests as u64);
            out.insert(
                "Ip.RoutingDiscards".to_string(),
                ip.dwRoutingDiscards as u64,
            );
            out.insert("Ip.OutDiscards".to_string(), ip.dwOutDiscards as u64);
            out.insert("Ip.OutNoRoutes".to_string(), ip.dwOutNoRoutes as u64);
            out.insert("Ip.ReasmReqds".to_string(), ip.dwReasmReqds as u64);
            out.insert("Ip.ReasmOKs".to_string(), ip.dwReasmOks as u64);
            out.insert("Ip.ReasmFails".to_string(), ip.dwReasmFails as u64);
            out.insert("Ip.FragOKs".to_string(), ip.dwFragOks as u64);
            out.insert("Ip.FragFails".to_string(), ip.dwFragFails as u64);
            out.insert("Ip.FragCreates".to_string(), ip.dwFragCreates as u64);
        }

        let mut tcp = MIB_TCPSTATS_LH::default();
        if GetTcpStatistics(&mut tcp) == 0 {
            out.insert("Tcp.ActiveOpens".to_string(), tcp.dwActiveOpens as u64);
            out.insert("Tcp.PassiveOpens".to_string(), tcp.dwPassiveOpens as u64);
            out.insert("Tcp.AttemptFails".to_string(), tcp.dwAttemptFails as u64);
            out.insert("Tcp.EstabResets".to_string(), tcp.dwEstabResets as u64);
            out.insert("Tcp.CurrEstab".to_string(), tcp.dwCurrEstab as u64);
            out.insert("Tcp.InSegs".to_string(), tcp.dwInSegs as u64);
            out.insert("Tcp.OutSegs".to_string(), tcp.dwOutSegs as u64);
            out.insert("Tcp.RetransSegs".to_string(), tcp.dwRetransSegs as u64);
            out.insert("Tcp.InErrs".to_string(), tcp.dwInErrs as u64);
            out.insert("Tcp.OutRsts".to_string(), tcp.dwOutRsts as u64);
        }

        let mut udp = MIB_UDPSTATS::default();
        if GetUdpStatistics(&mut udp) == 0 {
            out.insert("Udp.InDatagrams".to_string(), udp.dwInDatagrams as u64);
            out.insert("Udp.NoPorts".to_string(), udp.dwNoPorts as u64);
            out.insert("Udp.InErrors".to_string(), udp.dwInErrors as u64);
            out.insert("Udp.OutDatagrams".to_string(), udp.dwOutDatagrams as u64);
            out.insert("Udp.NumAddrs".to_string(), udp.dwNumAddrs as u64);
        }
    }
    out
}

fn collect_socket_counts() -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();
    unsafe {
        let mut size = 0u32;
        GetTcpTable2(None, &mut size, false);
        if size > 0 {
            let mut buf = vec![0u8; size as usize];
            if GetTcpTable2(
                Some(buf.as_mut_ptr() as *mut MIB_TCPTABLE2),
                &mut size,
                false,
            ) == 0
            {
                let table_ptr = buf.as_ptr() as *const MIB_TCPTABLE2;
                let count = std::ptr::addr_of!((*table_ptr).dwNumEntries).read_unaligned() as usize;
                let rows_ptr = std::ptr::addr_of!((*table_ptr).table) as *const MIB_TCPROW2;
                let mut established = 0u64;
                let mut listen = 0u64;
                let mut time_wait = 0u64;
                let mut close_wait = 0u64;
                let mut syn_sent = 0u64;
                let mut syn_recv = 0u64;
                let mut fin_wait1 = 0u64;
                let mut fin_wait2 = 0u64;
                let mut closing = 0u64;
                let mut last_ack = 0u64;
                for i in 0..count {
                    let row = rows_ptr.add(i).read_unaligned();
                    match MIB_TCP_STATE(row.dwState as i32) {
                        s if s == MIB_TCP_STATE_ESTAB => established += 1,
                        s if s == MIB_TCP_STATE_LISTEN => listen += 1,
                        s if s == MIB_TCP_STATE_TIME_WAIT => time_wait += 1,
                        s if s == MIB_TCP_STATE_CLOSE_WAIT => close_wait += 1,
                        s if s == MIB_TCP_STATE_SYN_SENT => syn_sent += 1,
                        s if s == MIB_TCP_STATE_SYN_RCVD => syn_recv += 1,
                        s if s == MIB_TCP_STATE_FIN_WAIT1 => fin_wait1 += 1,
                        s if s == MIB_TCP_STATE_FIN_WAIT2 => fin_wait2 += 1,
                        s if s == MIB_TCP_STATE_CLOSING => closing += 1,
                        s if s == MIB_TCP_STATE_LAST_ACK => last_ack += 1,
                        _ => {}
                    }
                }
                out.insert("v4.tcp.inuse".to_string(), established + listen);
                out.insert("v4.tcp.established".to_string(), established);
                out.insert("v4.tcp.listen".to_string(), listen);
                out.insert("v4.tcp.time_wait".to_string(), time_wait);
                out.insert("v4.tcp.close_wait".to_string(), close_wait);
                out.insert("v4.tcp.syn_sent".to_string(), syn_sent);
                out.insert("v4.tcp.syn_recv".to_string(), syn_recv);
                out.insert("v4.tcp.fin_wait1".to_string(), fin_wait1);
                out.insert("v4.tcp.fin_wait2".to_string(), fin_wait2);
                out.insert("v4.tcp.closing".to_string(), closing);
                out.insert("v4.tcp.last_ack".to_string(), last_ack);
                out.insert("v4.tcp.alloc".to_string(), count as u64);
            }
        }

        let mut size = 0u32;
        GetUdpTable(None, &mut size, false);
        if size > 0 {
            let mut buf = vec![0u8; size as usize];
            if GetUdpTable(
                Some(buf.as_mut_ptr() as *mut MIB_UDPTABLE),
                &mut size,
                false,
            ) == 0
            {
                let table_ptr = buf.as_ptr() as *const MIB_UDPTABLE;
                let count = std::ptr::addr_of!((*table_ptr).dwNumEntries).read_unaligned() as u64;
                out.insert("v4.udp.inuse".to_string(), count);
            }
        }

        let mut size = 0u32;
        GetTcp6Table2(std::ptr::null_mut(), &mut size, false);
        if size > 0 {
            let mut buf = vec![0u8; size as usize];
            if GetTcp6Table2(buf.as_mut_ptr() as *mut MIB_TCP6TABLE2, &mut size, false) == 0 {
                let table_ptr = buf.as_ptr() as *const MIB_TCP6TABLE2;
                let count = std::ptr::addr_of!((*table_ptr).dwNumEntries).read_unaligned() as usize;
                let rows_ptr = std::ptr::addr_of!((*table_ptr).table) as *const MIB_TCP6ROW2;
                let mut established = 0u64;
                let mut listen = 0u64;
                for i in 0..count {
                    let row = rows_ptr.add(i).read_unaligned();
                    match row.State {
                        s if s == MIB_TCP_STATE_ESTAB => established += 1,
                        s if s == MIB_TCP_STATE_LISTEN => listen += 1,
                        _ => {}
                    }
                }
                out.insert("v6.tcp.inuse".to_string(), established + listen);
                out.insert("v6.tcp.established".to_string(), established);
                out.insert("v6.tcp.listen".to_string(), listen);
                out.insert("v6.tcp.alloc".to_string(), count as u64);
            }
        }

        let mut size = 0u32;
        GetUdp6Table(None, &mut size, false);
        if size > 0 {
            let mut buf = vec![0u8; size as usize];
            if GetUdp6Table(
                Some(buf.as_mut_ptr() as *mut MIB_UDP6TABLE),
                &mut size,
                false,
            ) == 0
            {
                let table_ptr = buf.as_ptr() as *const MIB_UDP6TABLE;
                let count = std::ptr::addr_of!((*table_ptr).dwNumEntries).read_unaligned() as u64;
                out.insert("v6.udp.inuse".to_string(), count);
            }
        }
    }
    out
}

fn collect_interrupts_detail(per_cpu: &[CpuTimes]) -> BTreeMap<String, u64> {
    let _ = SYSTEM_INTERRUPT_INFORMATION_CLASS;
    let mut out = BTreeMap::new();
    for (cpu, times) in per_cpu.iter().enumerate() {
        out.insert(format!("isr_time_100ns|{cpu}"), times.irq);
    }
    out
}

fn collect_thread_state_vmstat(buf: &[u8]) -> BTreeMap<String, i64> {
    let mut out = BTreeMap::new();
    let mut running = 0u64;
    let mut ready = 0u64;
    let mut waiting = 0u64;
    let mut wait_reason_counts: BTreeMap<u32, u64> = BTreeMap::new();
    let spi_size = size_of::<SystemProcessInformation>();
    let sti_size = size_of::<SystemThreadInformation>();

    for (offset, spi) in walk_nt_list::<SystemProcessInformation>(buf) {
        let thread_count = spi.number_of_threads as usize;
        let mut thread_offset = offset + spi_size;

        for _ in 0..thread_count {
            if let Some(thread) =
                read_unaligned_struct::<SystemThreadInformation>(buf, thread_offset)
            {
                match thread.thread_state {
                    THREAD_STATE_RUNNING => running += 1,
                    THREAD_STATE_READY => ready += 1,
                    THREAD_STATE_WAIT => {
                        waiting += 1;
                        let entry = wait_reason_counts.entry(thread.wait_reason).or_insert(0);
                        *entry = entry.saturating_add(1);
                    }
                    _ => {}
                }
            }
            thread_offset += sti_size;
        }
    }

    out.insert("windows.thread.state.running".to_string(), running as i64);
    out.insert("windows.thread.state.ready".to_string(), ready as i64);
    out.insert("windows.thread.state.wait".to_string(), waiting as i64);
    for (reason, count) in wait_reason_counts {
        out.insert(
            format!("windows.thread.wait_reason.{}", reason),
            count as i64,
        );
    }
    out
}

fn collect_numa_vmstat() -> BTreeMap<String, i64> {
    let mut out = BTreeMap::new();
    let mut highest = 0u32;
    let ok = unsafe { GetNumaHighestNodeNumber(&mut highest) }.is_ok();
    if ok {
        out.insert(
            "windows.numa.nodes".to_string(),
            highest.saturating_add(1) as i64,
        );
    }
    out
}

fn collect_softirqs_detail(per_cpu: &[CpuTimes]) -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();
    for (cpu, times) in per_cpu.iter().enumerate() {
        out.insert(format!("dpc_time_100ns|{cpu}"), times.softirq);
    }
    out
}

fn extract_process_thread_summaries(buf: &[u8]) -> (u64, u32, BTreeMap<i32, ProcessThreadSummary>) {
    let mut count = 0u64;
    let mut procs_blocked = 0u32;
    let mut summaries: BTreeMap<i32, ProcessThreadSummary> = BTreeMap::new();
    let spi_size = size_of::<SystemProcessInformation>();
    let sti_size = size_of::<SystemThreadInformation>();

    for (offset, spi) in walk_nt_list::<SystemProcessInformation>(buf) {
        let pid = spi.unique_process_id.0 as usize as i32;
        count += 1;

        let thread_count = spi.number_of_threads as usize;
        let threads_base = offset + spi_size;
        let mut any_running = false;
        let mut any_ready = false;
        let mut all_waiting = thread_count > 0;
        let mut any_blocked = false;

        for t in 0..thread_count {
            let t_off = threads_base + t * sti_size;
            let ti = match read_unaligned_struct::<SystemThreadInformation>(buf, t_off) {
                Some(ti) => ti,
                None => break,
            };
            match ti.thread_state {
                THREAD_STATE_RUNNING => {
                    any_running = true;
                    all_waiting = false;
                }
                THREAD_STATE_READY => {
                    any_ready = true;
                    all_waiting = false;
                }
                THREAD_STATE_WAIT => {
                    if ti.wait_reason == 0 || ti.wait_reason == 14 {
                        any_blocked = true;
                    }
                }
                _ => {
                    all_waiting = false;
                }
            }
        }

        if any_blocked && !any_running && !any_ready {
            procs_blocked += 1;
        }

        let state = if any_running || any_ready {
            "R".to_string()
        } else if all_waiting {
            "S".to_string()
        } else {
            "unknown".to_string()
        };

        summaries.insert(
            pid,
            ProcessThreadSummary {
                state,
                last_cpu: None,
            },
        );
    }

    (count.saturating_sub(1), procs_blocked, summaries)
}

