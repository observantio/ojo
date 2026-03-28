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

pub fn netstat_attrs(key: &str) -> Option<Vec<KeyValue>> {
    let (family, metric) = key.split_once('.')?;
    Some(vec![
        KeyValue::new("family", family.to_string()),
        KeyValue::new("key", metric.to_string()),
    ])
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

pub fn schedstat_attrs(key: &str) -> Option<Vec<KeyValue>> {
    let parts = key.split('|').collect::<Vec<_>>();
    if parts.len() != 3 || parts[0] != "cpu" {
        return None;
    }

    Some(vec![
        KeyValue::new("cpu", parts[2].to_string()),
        KeyValue::new("key", parts[1].to_string()),
    ])
}

pub fn runqueue_attrs(key: &str) -> Option<Vec<KeyValue>> {
    let (scope, cpu) = key.split_once('|')?;
    if scope != "cpu" {
        return None;
    }
    Some(vec![KeyValue::new("cpu", cpu.to_string())])
}

pub fn slabinfo_attrs(key: &str) -> Option<Vec<KeyValue>> {
    let parts = key.split('|').collect::<Vec<_>>();
    if parts.len() != 3 {
        return None;
    }

    Some(vec![
        KeyValue::new("cache", parts[0].to_string()),
        KeyValue::new("key", parts[1].to_string()),
        KeyValue::new("unit", parts[2].to_string()),
    ])
}

pub fn filesystem_attrs(key: &str) -> Option<Vec<KeyValue>> {
    let parts = key.split('|').collect::<Vec<_>>();
    if parts.len() != 3 {
        return None;
    }

    Some(vec![
        KeyValue::new("mountpoint", parts[0].to_string()),
        KeyValue::new("key", parts[1].to_string()),
        KeyValue::new("unit", parts[2].to_string()),
    ])
}

pub fn cgroup_attrs(key: &str) -> Option<Vec<KeyValue>> {
    let parts = key.split('|').collect::<Vec<_>>();
    if parts.len() < 3 {
        return None;
    }

    let mut attrs = vec![
        KeyValue::new("scope", parts[0].to_string()),
        KeyValue::new("section", parts[1].to_string()),
    ];

    if parts.len() == 3 {
        attrs.push(KeyValue::new("key", parts[2].to_string()));
    } else {
        attrs.push(KeyValue::new("device", parts[2].to_string()));
        attrs.push(KeyValue::new("key", parts[3].to_string()));
    }

    Some(attrs)
}

#[cfg(test)]
#[path = "tests/catalog_tests.rs"]
mod tests;
