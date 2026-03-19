impl ProcMetrics {
    fn record_load(&self, snap: &Snapshot) {
        let Some(load) = snap.load.as_ref() else {
            return;
        };
        self.record_f64("system.cpu.load_average.1m", &self.load_1m, load.one, &[]);
        self.record_f64("system.cpu.load_average.5m", &self.load_5m, load.five, &[]);
        self.record_f64(
            "system.cpu.load_average.15m",
            &self.load_15m,
            load.fifteen,
            &[],
        );
        if !snap.system.is_windows {
            self.record_u64(
                "system.linux.load.runnable",
                &self.load_runnable,
                non_negative_u64(load.runnable),
                &[],
            );
            self.record_u64(
                "system.linux.load.entities",
                &self.load_entities,
                non_negative_u64(load.entities),
                &[],
            );
            self.record_u64(
                "system.linux.load.latest_pid",
                &self.load_latest_pid,
                non_negative_u64(load.latest_pid),
                &[],
            );
        }
    }

    fn record_memory(&self, snap: &Snapshot, derived: &DerivedMetrics) {
        let is_windows = snap.system.is_windows;
        let m = &snap.memory;

        self.record_u64(
            "system.memory.total",
            &self.mem_total_bytes,
            m.mem_total_bytes,
            &[],
        );
        self.record_u64(
            "system.memory.free",
            &self.mem_free_bytes,
            m.mem_free_bytes,
            &[],
        );
        self.record_u64(
            "system.memory.available",
            &self.mem_available_bytes,
            m.mem_available_bytes,
            &[],
        );
        if !is_windows {
            if let Some(value) = m.buffers_bytes {
                self.record_u64("system.memory.buffers", &self.mem_buffers_bytes, value, &[]);
            }
        }
        self.record_u64(
            "system.memory.cached",
            &self.mem_cached_bytes,
            m.cached_bytes,
            &[],
        );
        if !is_windows {
            if let Some(value) = m.active_bytes {
                self.record_u64("system.memory.active", &self.mem_active_bytes, value, &[]);
            }
            if let Some(value) = m.inactive_bytes {
                self.record_u64(
                    "system.memory.inactive",
                    &self.mem_inactive_bytes,
                    value,
                    &[],
                );
            }
            if let Some(value) = m.anon_pages_bytes {
                self.record_u64("system.memory.anon", &self.mem_anon_bytes, value, &[]);
            }
            if let Some(value) = m.mapped_bytes {
                self.record_u64("system.memory.mapped", &self.mem_mapped_bytes, value, &[]);
            }
            if let Some(value) = m.shmem_bytes {
                self.record_u64("system.memory.shmem", &self.mem_shmem_bytes, value, &[]);
            }
        }
        self.record_u64(
            "system.swap.total",
            &self.swap_total_bytes,
            m.swap_total_bytes,
            &[],
        );
        self.record_u64(
            "system.swap.free",
            &self.swap_free_bytes,
            m.swap_free_bytes,
            &[],
        );
        if !is_windows {
            if let Some(value) = m.swap_cached_bytes {
                self.record_u64("system.swap.cached", &self.swap_cached_bytes, value, &[]);
            }
            if let Some(value) = m.dirty_bytes {
                self.record_u64("system.memory.dirty", &self.mem_dirty_bytes, value, &[]);
            }
            if let Some(value) = m.writeback_bytes {
                self.record_u64(
                    "system.memory.writeback",
                    &self.mem_writeback_bytes,
                    value,
                    &[],
                );
            }
            if let Some(value) = m.slab_bytes {
                self.record_u64("system.memory.slab", &self.mem_slab_bytes, value, &[]);
            }
            if let Some(value) = m.sreclaimable_bytes {
                self.record_u64(
                    "system.memory.sreclaimable",
                    &self.mem_sreclaimable_bytes,
                    value,
                    &[],
                );
            }
            if let Some(value) = m.sunreclaim_bytes {
                self.record_u64(
                    "system.memory.sunreclaim",
                    &self.mem_sunreclaim_bytes,
                    value,
                    &[],
                );
            }
            if let Some(value) = m.page_tables_bytes {
                self.record_u64(
                    "system.memory.page_tables",
                    &self.mem_page_tables_bytes,
                    value,
                    &[],
                );
            }
        }
        self.record_u64(
            "system.memory.commit_limit",
            &self.mem_commit_limit_bytes,
            m.commit_limit_bytes,
            &[],
        );
        self.record_u64(
            "system.memory.committed_as",
            &self.mem_committed_as_bytes,
            m.committed_as_bytes,
            &[],
        );
        if !is_windows {
            if let Some(value) = m.kernel_stack_bytes {
                self.record_u64(
                    "system.memory.kernel_stack",
                    &self.mem_kernel_stack_bytes,
                    value,
                    &[],
                );
            }
            if let Some(value) = m.anon_hugepages_bytes {
                self.record_u64(
                    "system.memory.anon_hugepages",
                    &self.mem_anon_hugepages_bytes,
                    value,
                    &[],
                );
            }
            if let Some(value) = m.hugepages_total {
                self.record_u64(
                    "system.memory.hugepages_total",
                    &self.mem_hugepages_total,
                    value,
                    &[],
                );
            }
            if let Some(value) = m.hugepages_free {
                self.record_u64(
                    "system.memory.hugepages_free",
                    &self.mem_hugepages_free,
                    value,
                    &[],
                );
            }
            if let Some(value) = m.hugepage_size_bytes {
                self.record_u64(
                    "system.memory.hugepage_size",
                    &self.mem_hugepage_size_bytes,
                    value,
                    &[],
                );
            }
        }

        self.record_f64(
            "system.memory.used_ratio",
            &self.mem_used_ratio,
            derived.memory_used_ratio,
            &[],
        );
        self.record_f64(
            "system.swap.used_ratio",
            &self.swap_used_ratio,
            derived.swap_used_ratio,
            &[],
        );
        if !is_windows {
            self.record_f64(
                "system.memory.dirty_writeback_ratio",
                &self.mem_dirty_writeback_ratio,
                derived.dirty_writeback_ratio,
                &[],
            );
        }
    }

    fn record_paging(&self, derived: &DerivedMetrics) {
        self.record_f64(
            "system.paging.faults_per_sec",
            &self.page_faults_per_sec,
            derived.page_faults_per_sec,
            &[],
        );
        self.record_f64(
            "system.paging.major_faults_per_sec",
            &self.major_page_faults_per_sec,
            derived.major_page_faults_per_sec,
            &[],
        );
        self.record_f64(
            "system.paging.page_ins_per_sec",
            &self.page_ins_per_sec,
            derived.page_ins_per_sec,
            &[],
        );
        self.record_f64(
            "system.paging.page_outs_per_sec",
            &self.page_outs_per_sec,
            derived.page_outs_per_sec,
            &[],
        );
        self.record_f64(
            "system.swap.ins_per_sec",
            &self.swap_ins_per_sec,
            derived.swap_ins_per_sec,
            &[],
        );
        self.record_f64(
            "system.swap.outs_per_sec",
            &self.swap_outs_per_sec,
            derived.swap_outs_per_sec,
            &[],
        );
    }

}
