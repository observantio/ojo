fn read_proc_first_value(cache: &mut ReadCache, path: impl AsRef<Path>) -> Option<String> {
    cache
        .read_raw(path)?
        .split_whitespace()
        .next()
        .map(str::to_string)
}

fn read_proc_u64(cache: &mut ReadCache, path: impl AsRef<Path>) -> Option<u64> {
    read_proc_first_value(cache, path)?.parse().ok()
}

#[derive(Default)]
struct ProcessStatusFields {
    fd_table_size: Option<u64>,
    vm_size_kib: Option<u64>,
    vm_rss_kib: Option<u64>,
    vm_data_kib: Option<u64>,
    vm_stack_kib: Option<u64>,
    vm_exe_kib: Option<u64>,
    vm_lib_kib: Option<u64>,
    vm_swap_kib: Option<u64>,
    vm_pte_kib: Option<u64>,
    vm_hwm_kib: Option<u64>,
    voluntary_ctxt_switches: Option<u64>,
    nonvoluntary_ctxt_switches: Option<u64>,
}

fn parse_status_value_kib(raw: &str) -> Option<u64> {
    raw.split_whitespace().next()?.parse().ok()
}

fn parse_process_status_fields(contents: &str) -> ProcessStatusFields {
    let mut out = ProcessStatusFields::default();

    for line in contents.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();

        match key {
            "FDSize" => out.fd_table_size = value.parse().ok(),
            "VmSize" => out.vm_size_kib = parse_status_value_kib(value),
            "VmRSS" => out.vm_rss_kib = parse_status_value_kib(value),
            "VmData" => out.vm_data_kib = parse_status_value_kib(value),
            "VmStk" => out.vm_stack_kib = parse_status_value_kib(value),
            "VmExe" => out.vm_exe_kib = parse_status_value_kib(value),
            "VmLib" => out.vm_lib_kib = parse_status_value_kib(value),
            "VmSwap" => out.vm_swap_kib = parse_status_value_kib(value),
            "VmPTE" => out.vm_pte_kib = parse_status_value_kib(value),
            "VmHWM" => out.vm_hwm_kib = parse_status_value_kib(value),
            "voluntary_ctxt_switches" => out.voluntary_ctxt_switches = value.parse().ok(),
            "nonvoluntary_ctxt_switches" => out.nonvoluntary_ctxt_switches = value.parse().ok(),
            _ => {}
        }
    }

    out
}
