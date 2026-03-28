impl ProcMetrics {
    fn record_processes(&self, snap: &Snapshot, derived: &DerivedMetrics) {
        let is_windows = snap.system.is_windows;
        let is_linux = is_linux_like(snap);
        for proc in &snap.processes {
            if proc.comm.is_empty() {
                continue;
            }
            let base_attrs = self.process_base_attrs(proc);

            if let Some(cpu) = derived.process_cpu_ratio.get(&proc.pid) {
                self.record_f64(
                    "process.cpu.utilization",
                    &self.process_cpu_ratio,
                    *cpu,
                    &base_attrs,
                );
            }

            if let Some(rss_bytes) = process_rss_bytes(proc, is_windows) {
                self.record_u64(
                    "process.memory.rss",
                    &self.process_rss_bytes,
                    rss_bytes,
                    &base_attrs,
                );
            }

            self.record_i64(
                "process.parent_pid",
                &self.process_ppid,
                proc.ppid as i64,
                &base_attrs,
            );
            self.record_i64(
                "process.thread.count",
                &self.process_num_threads,
                proc.num_threads,
                &base_attrs,
            );
            if is_windows {
                self.record_i64(
                    "process.priority",
                    &self.process_priority,
                    proc.priority,
                    &base_attrs,
                );
                if let Some(value) = proc.read_bytes {
                    self.record_u64(
                        "process.io.read_bytes",
                        &self.process_read_bytes,
                        value,
                        &base_attrs,
                    );
                }
                if let Some(value) = proc.write_bytes {
                    self.record_u64(
                        "process.io.write_bytes",
                        &self.process_write_bytes,
                        value,
                        &base_attrs,
                    );
                }
                if let Some(value) = proc.virtual_size_bytes {
                    self.record_u64(
                        "process.memory.vm_size",
                        &self.process_vm_size_bytes,
                        value,
                        &base_attrs,
                    );
                }
                if let Some(value) = proc.working_set_bytes {
                    self.record_u64(
                        "process.memory.working_set",
                        &self.process_working_set_bytes,
                        value,
                        &base_attrs,
                    );
                }
                if let Some(value) = proc.peak_working_set_bytes {
                    self.record_u64(
                        "process.memory.peak_working_set",
                        &self.process_peak_working_set_bytes,
                        value,
                        &base_attrs,
                    );
                }
                if let Some(value) = proc.pagefile_usage_bytes {
                    self.record_u64(
                        "process.memory.pagefile_usage",
                        &self.process_pagefile_usage_bytes,
                        value,
                        &base_attrs,
                    );
                }
                if let Some(value) = proc.private_bytes {
                    self.record_u64(
                        "process.memory.private_bytes",
                        &self.process_private_bytes,
                        value,
                        &base_attrs,
                    );
                }
                if let Some(value) = proc.commit_charge_bytes {
                    self.record_u64(
                        "process.memory.commit_charge",
                        &self.process_commit_charge_bytes,
                        value,
                        &base_attrs,
                    );
                }
            } else {
                self.record_i64(
                    "process.priority",
                    &self.process_priority,
                    proc.priority,
                    &base_attrs,
                );
                if is_linux {
                    self.record_i64(
                        "process.linux.nice",
                        &self.process_nice,
                        proc.nice,
                        &base_attrs,
                    );
                }
                self.record_u64(
                    "process.memory.virtual",
                    &self.process_vsize_bytes,
                    proc.vsize_bytes,
                    &base_attrs,
                );

                if let Some(value) = proc.read_bytes {
                    self.record_u64(
                        "process.io.read_bytes",
                        &self.process_read_bytes,
                        value,
                        &base_attrs,
                    );
                }
                if let Some(value) = proc.write_bytes {
                    self.record_u64(
                        "process.io.write_bytes",
                        &self.process_write_bytes,
                        value,
                        &base_attrs,
                    );
                }
                if let (true, Some(value)) = (is_linux, proc.cancelled_write_bytes) {
                    self.record_i64(
                        "process.linux.io.cancelled_write_bytes",
                        &self.process_cancelled_write_bytes,
                        value,
                        &base_attrs,
                    );
                }
                if let Some(value) = proc.vm_size_kib {
                    self.record_u64(
                        "process.memory.vm_size",
                        &self.process_vm_size_bytes,
                        kib_to_bytes(value),
                        &base_attrs,
                    );
                }
                if let Some(value) = proc.vm_rss_kib {
                    self.record_u64(
                        "process.memory.vm_rss",
                        &self.process_vm_rss_bytes,
                        kib_to_bytes(value),
                        &base_attrs,
                    );
                }
            }

            if let Some(value) = derived.process_cpu_user_delta_secs.get(&proc.pid) {
                let attrs = self.process_attrs_with(proc, &[KeyValue::new(ATTR_CPU_MODE, "user")]);
                self.add_f64(
                    "process.cpu.time",
                    &self.otel_process_cpu_time,
                    *value,
                    &attrs,
                );
            }
            if let Some(value) = derived.process_cpu_system_delta_secs.get(&proc.pid) {
                let attrs =
                    self.process_attrs_with(proc, &[KeyValue::new(ATTR_CPU_MODE, "system")]);
                self.add_f64(
                    "process.cpu.time",
                    &self.otel_process_cpu_time,
                    *value,
                    &attrs,
                );
            }
            if let Some(value) = derived.process_read_bytes_delta.get(&proc.pid) {
                let attrs =
                    self.process_attrs_with(proc, &[KeyValue::new(ATTR_DISK_IO_DIRECTION, "read")]);
                self.add_u64(
                    "process.disk.io",
                    &self.otel_process_io,
                    *value,
                    &attrs,
                );
            }
            if let Some(value) = derived.process_write_bytes_delta.get(&proc.pid) {
                let attrs = self
                    .process_attrs_with(proc, &[KeyValue::new(ATTR_DISK_IO_DIRECTION, "write")]);
                self.add_u64(
                    "process.disk.io",
                    &self.otel_process_io,
                    *value,
                    &attrs,
                );
            }
            if let Some(value) = derived.process_read_chars_delta.get(&proc.pid) {
                let attrs =
                    self.process_attrs_with(proc, &[KeyValue::new(ATTR_DISK_IO_DIRECTION, "read")]);
                self.add_u64(
                    "process.io.chars",
                    &self.otel_process_io_chars,
                    *value,
                    &attrs,
                );
            }
            if let Some(value) = derived.process_write_chars_delta.get(&proc.pid) {
                let attrs = self
                    .process_attrs_with(proc, &[KeyValue::new(ATTR_DISK_IO_DIRECTION, "write")]);
                self.add_u64(
                    "process.io.chars",
                    &self.otel_process_io_chars,
                    *value,
                    &attrs,
                );
            }
            if let Some(value) = derived.process_syscr_delta.get(&proc.pid) {
                let attrs =
                    self.process_attrs_with(proc, &[KeyValue::new(ATTR_DISK_IO_DIRECTION, "read")]);
                self.add_u64(
                    "process.io.syscalls",
                    &self.otel_process_io_syscalls,
                    *value,
                    &attrs,
                );
            }
            if let Some(value) = derived.process_syscw_delta.get(&proc.pid) {
                let attrs = self
                    .process_attrs_with(proc, &[KeyValue::new(ATTR_DISK_IO_DIRECTION, "write")]);
                self.add_u64(
                    "process.io.syscalls",
                    &self.otel_process_io_syscalls,
                    *value,
                    &attrs,
                );
            }
            if let Some(value) = derived.process_voluntary_ctxt_delta.get(&proc.pid) {
                let attrs = self.process_attrs_with(proc, &[KeyValue::new("type", "voluntary")]);
                self.add_u64(
                    "process.context_switches",
                    &self.otel_process_context_switches,
                    *value,
                    &attrs,
                );
            }
            if let Some(value) = derived.process_nonvoluntary_ctxt_delta.get(&proc.pid) {
                let attrs = self.process_attrs_with(proc, &[KeyValue::new("type", "involuntary")]);
                self.add_u64(
                    "process.context_switches",
                    &self.otel_process_context_switches,
                    *value,
                    &attrs,
                );
            }
            if let Some(value) = derived.process_minor_faults_delta.get(&proc.pid) {
                let attrs = self.process_attrs_with(proc, &[KeyValue::new("type", "minor")]);
                self.add_u64(
                    "process.paging.faults",
                    &self.otel_process_page_faults,
                    *value,
                    &attrs,
                );
            }
            if let Some(value) = derived.process_major_faults_delta.get(&proc.pid) {
                let attrs = self.process_attrs_with(proc, &[KeyValue::new("type", "major")]);
                self.add_u64(
                    "process.paging.faults",
                    &self.otel_process_page_faults,
                    *value,
                    &attrs,
                );
            }

            if let Some(value) = proc.fd_count {
                self.record_u64(
                    "process.unix.file_descriptor.count",
                    &self.otel_process_unix_file_descriptor_count,
                    value,
                    &base_attrs,
                );
            }
            if !is_windows {
                if let Some(value) = proc.oom_score {
                    self.record_i64(
                        "process.oom_score",
                        &self.otel_process_oom_score,
                        value,
                        &base_attrs,
                    );
                }
                if let Some(value) = proc.processor {
                    self.record_i64(
                        "process.cpu.last_id",
                        &self.otel_process_processor,
                        value,
                        &base_attrs,
                    );
                }
            }

            let start_time_unix = snap.system.boot_time_epoch_secs as f64
                + (proc.start_time_ticks as f64 / snap.system.ticks_per_second.max(1) as f64);

            self.record_f64(
                "process.start_time",
                &self.otel_process_start_time,
                start_time_unix,
                &base_attrs,
            );
            if is_linux {
                self.record_u64(
                    "process.linux.start_time",
                    &self.otel_process_start_time_ticks,
                    proc.start_time_ticks,
                    &base_attrs,
                );
            }

            if is_linux {
                if let Some(value) = proc.rt_priority {
                    let attrs = self.process_attrs_with(
                        proc,
                        &[KeyValue::new("field", "rt_priority")],
                    );
                    self.record_u64(
                        "process.linux.scheduler",
                        &self.otel_process_sched_priority,
                        value,
                        &attrs,
                    );
                }
                if let Some(value) = proc.policy {
                    let attrs = self.process_attrs_with(proc, &[KeyValue::new("field", "policy")]);
                    self.record_u64(
                        "process.linux.scheduler",
                        &self.otel_process_sched_priority,
                        value,
                        &attrs,
                    );
                }
            }

            if let Some(rss_bytes) = process_rss_bytes(proc, is_windows) {
                self.record_u64(
                    "process.memory.usage",
                    &self.otel_process_memory_usage,
                    rss_bytes,
                    &self.process_attrs_with(proc, &[KeyValue::new("type", "rss")]),
                );
            }

            self.record_u64(
                "process.memory.usage",
                &self.otel_process_memory_usage,
                proc.virtual_size_bytes.unwrap_or(proc.vsize_bytes),
                &self.process_attrs_with(proc, &[KeyValue::new("type", "virtual")]),
            );

            if is_windows {
                for (kind, maybe_value) in [
                    ("working_set", proc.working_set_bytes),
                    ("private_bytes", proc.private_bytes),
                    ("peak_working_set", proc.peak_working_set_bytes),
                    ("pagefile_usage", proc.pagefile_usage_bytes),
                    ("commit_charge", proc.commit_charge_bytes),
                ] {
                    if let Some(value) = maybe_value {
                        self.record_u64(
                            "process.memory.usage",
                            &self.otel_process_memory_usage,
                            value,
                            &self.process_attrs_with(proc, &[KeyValue::new("type", kind)]),
                        );
                    }
                }
            } else {
                for (kind, maybe_value) in [
                    ("vm_size", proc.vm_size_kib),
                    ("vm_rss", proc.vm_rss_kib),
                    ("vm_data", proc.vm_data_kib),
                    ("vm_stack", proc.vm_stack_kib),
                    ("vm_exe", proc.vm_exe_kib),
                    ("vm_lib", proc.vm_lib_kib),
                    ("vm_swap", proc.vm_swap_kib),
                    ("vm_pte", proc.vm_pte_kib),
                    ("vm_hwm", proc.vm_hwm_kib),
                ] {
                    if let Some(value) = maybe_value {
                        self.record_u64(
                            "process.memory.usage",
                            &self.otel_process_memory_usage,
                            kib_to_bytes(value),
                            &self.process_attrs_with(proc, &[KeyValue::new("type", kind)]),
                        );
                    }
                }
            }
        }
    }
}
