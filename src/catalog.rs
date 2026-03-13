use opentelemetry::KeyValue;

pub fn pressure_attrs(key: &str) -> Option<Vec<KeyValue>> {
    let parts = key.split('.').collect::<Vec<_>>();
    if parts.len() != 3 {
        return None;
    }

    Some(vec![
        KeyValue::new("resource", parts[0].to_string()),
        KeyValue::new("scope", parts[1].to_string()),
        KeyValue::new("window", parts[2].to_string()),
    ])
}

pub fn pressure_stall_time_attrs(key: &str) -> Option<Vec<KeyValue>> {
    let parts = key.split('.').collect::<Vec<_>>();
    if parts.len() != 2 {
        return None;
    }

    Some(vec![
        KeyValue::new("resource", parts[0].to_string()),
        KeyValue::new("scope", parts[1].to_string()),
    ])
}

pub fn vmstat_attrs(key: &str) -> Vec<KeyValue> {
    vec![KeyValue::new("key", key.to_string())]
}

pub fn net_snmp_attrs(key: &str) -> Vec<KeyValue> {
    vec![KeyValue::new("key", key.to_string())]
}

pub fn interrupts_attrs(key: &str) -> Option<Vec<KeyValue>> {
    let (irq, cpu) = key.split_once('|')?;
    Some(vec![
        KeyValue::new("irq", irq.to_string()),
        KeyValue::new("cpu", cpu.to_string()),
    ])
}

pub fn softirqs_attrs(key: &str) -> Option<Vec<KeyValue>> {
    let (kind, cpu) = key.split_once('|')?;
    Some(vec![
        KeyValue::new("type", kind.to_string()),
        KeyValue::new("cpu", cpu.to_string()),
    ])
}

pub fn zoneinfo_attrs(key: &str) -> Option<Vec<KeyValue>> {
    let parts = key.split('|').collect::<Vec<_>>();
    if parts.len() != 3 {
        return None;
    }

    Some(vec![
        KeyValue::new("node", parts[0].to_string()),
        KeyValue::new("zone", parts[1].to_string()),
        KeyValue::new("key", parts[2].to_string()),
    ])
}

pub fn buddyinfo_attrs(key: &str) -> Option<Vec<KeyValue>> {
    let parts = key.split('|').collect::<Vec<_>>();
    if parts.len() != 3 {
        return None;
    }

    Some(vec![
        KeyValue::new("node", parts[0].to_string()),
        KeyValue::new("zone", parts[1].to_string()),
        KeyValue::new("order", parts[2].to_string()),
    ])
}
