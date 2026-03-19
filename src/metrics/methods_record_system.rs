impl ProcMetrics {
    pub fn record(&self, snap: &Snapshot, derived: &DerivedMetrics, include_processes: bool) {
        self.record_metadata(snap);
        self.record_system(snap, derived);
        self.record_load(snap);
        self.record_memory(snap, derived);
        self.record_paging(derived);
        self.record_pressure(snap, derived);
        self.record_stat(snap);
        self.record_cpu_inventory(snap);
        self.record_mounts(snap);
        self.record_filesystem_usage(snap);
        self.record_linux_extended(snap);
        self.record_linux_proc(snap, derived);
        self.record_net_kernel(snap, derived);
        self.record_disks(snap, derived);
        self.record_network_interfaces(snap, derived);
        if include_processes {
            self.record_processes(snap, derived);
        }
    }

    fn record_metadata(&self, snap: &Snapshot) {
        for (metric, state) in &snap.support_state {
            self.record_u64(
                "system.metric.support_state",
                &self.metric_support_state,
                1,
                &[
                    KeyValue::new("metric", metric.clone()),
                    KeyValue::new("state", state.clone()),
                ],
            );
        }
        for (metric, class) in &snap.metric_classification {
            self.record_u64(
                "system.metric.classification",
                &self.metric_classification_state,
                1,
                &[
                    KeyValue::new("metric", metric.clone()),
                    KeyValue::new("class", class.clone()),
                ],
            );
        }
    }

    fn record_system(&self, snap: &Snapshot, derived: &DerivedMetrics) {
        let is_windows = snap.system.is_windows;
        self.record_f64(
            "system.uptime",
            &self.otel_system_uptime,
            snap.system.uptime_secs,
            &[],
        );

        self.record_u64(
            "system.process.count",
            &self.otel_system_process_count,
            snap.system.process_count,
            &[KeyValue::new("state", "all")],
        );
        self.record_u64(
            "system.processes.count",
            &self.otel_system_processes,
            snap.system.process_count,
            &[KeyValue::new("state", "all")],
        );

        self.record_u64(
            "system.process.count",
            &self.otel_system_process_count,
            non_negative_u64(snap.system.procs_running),
            &[KeyValue::new("state", "running")],
        );
        self.record_u64(
            "system.processes.count",
            &self.otel_system_processes,
            non_negative_u64(snap.system.procs_running),
            &[KeyValue::new("state", "running")],
        );
        if !is_windows {
            self.record_u64(
                "system.process.count",
                &self.otel_system_process_count,
                non_negative_u64(snap.system.procs_blocked),
                &[KeyValue::new("state", "blocked")],
            );
            self.record_u64(
                "system.processes.count",
                &self.otel_system_processes,
                non_negative_u64(snap.system.procs_blocked),
                &[KeyValue::new("state", "blocked")],
            );
        }

        if !is_windows {
            self.record_u64(
                "system.linux.pid.max",
                &self.otel_system_pid_max,
                snap.system.pid_max.unwrap_or(0),
                &[],
            );
            self.record_u64(
                "system.linux.entropy",
                &self.otel_system_entropy,
                snap.system.entropy_available_bits.unwrap_or(0),
                &[KeyValue::new("state", "available")],
            );
            self.record_u64(
                "system.linux.entropy",
                &self.otel_system_entropy,
                snap.system.entropy_pool_size_bits.unwrap_or(0),
                &[KeyValue::new("state", "pool_size")],
            );
        }

        self.add_u64(
            "system.cpu.interrupts",
            &self.otel_system_interrupts,
            derived.interrupts_delta,
            &[],
        );
        self.add_u64(
            "system.cpu.softirqs",
            &self.otel_system_softirqs,
            derived.softirqs_delta,
            &[],
        );
        self.add_u64(
            "system.context_switches",
            &self.otel_system_context_switches,
            derived.context_switches_delta,
            &[],
        );
        self.add_u64(
            "system.processes.created",
            &self.otel_system_processes_created,
            derived.forks_delta,
            &[],
        );

        for (state, value) in &derived.cpu_time_delta_secs {
            if is_windows && *state == "iowait" {
                continue;
            }
            self.add_f64(
                "system.cpu.time",
                &self.otel_system_cpu_time,
                *value,
                &[KeyValue::new("state", (*state).to_string())],
            );
        }

        self.add_u64(
            "system.paging.faults",
            &self.otel_system_paging_faults,
            derived.page_faults_delta,
            &[KeyValue::new("type", "minor")],
        );
        self.add_u64(
            "system.paging.faults",
            &self.otel_system_paging_faults,
            derived.major_page_faults_delta,
            &[KeyValue::new("type", "major")],
        );
        self.add_u64(
            "system.paging.operations",
            &self.otel_system_paging_operations,
            derived.page_ins_delta,
            &[KeyValue::new("direction", "in")],
        );
        self.add_u64(
            "system.paging.operations",
            &self.otel_system_paging_operations,
            derived.page_outs_delta,
            &[KeyValue::new("direction", "out")],
        );
        self.add_u64(
            "system.swap.operations",
            &self.otel_system_swap_operations,
            derived.swap_ins_delta,
            &[KeyValue::new("direction", "in")],
        );
        self.add_u64(
            "system.swap.operations",
            &self.otel_system_swap_operations,
            derived.swap_outs_delta,
            &[KeyValue::new("direction", "out")],
        );

        self.record_f64(
            "system.cpu.utilization",
            &self.cpu_utilization,
            derived.cpu_utilization_ratio,
            &[],
        );

        for (cpu, ratio) in &derived.per_cpu_utilization_ratio {
            self.record_f64(
                "system.cpu.core.utilization",
                &self.per_cpu_utilization,
                *ratio,
                &[KeyValue::new("cpu", cpu.to_string())],
            );
        }
        if !is_windows {
            for (cpu, ratio) in &derived.per_cpu_iowait_ratio {
                self.record_f64(
                    "system.cpu.core.iowait_ratio",
                    &self.per_cpu_iowait,
                    *ratio,
                    &[KeyValue::new("cpu", cpu.to_string())],
                );
            }
        }
        for (cpu, ratio) in &derived.per_cpu_system_ratio {
            self.record_f64(
                "system.cpu.core.system_ratio",
                &self.per_cpu_system,
                *ratio,
                &[KeyValue::new("cpu", cpu.to_string())],
            );
        }
    }

}
