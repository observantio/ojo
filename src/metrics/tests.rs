#[cfg(test)]
mod tests {
    use crate::metrics::{MetricFilter, ProcessLabelConfig};

    #[test]
    fn metric_filter_allows_all_when_no_rules() {
        let filter = MetricFilter::new(vec![], vec![]);
        assert!(filter.enabled("system.cpu.time"));
        assert!(filter.enabled("process.memory.usage"));
    }

    #[test]
    fn metric_filter_respects_include_prefixes() {
        let filter = MetricFilter::new(vec!["system.".to_string()], vec![]);
        assert!(filter.enabled("system.cpu.time"));
        assert!(!filter.enabled("process.cpu.time"));
    }

    #[test]
    fn metric_filter_exclude_wins_over_include() {
        let filter = MetricFilter::new(
            vec!["system.".to_string(), "process.".to_string()],
            vec!["process.".to_string()],
        );
        assert!(filter.enabled("system.cpu.time"));
        assert!(!filter.enabled("process.cpu.time"));
    }

    #[test]
    fn metric_filter_matches_exact_and_group_roots() {
        let filter = MetricFilter::new(vec!["system.cpu".to_string()], vec![]);
        assert!(filter.enabled("system.cpu.time"));
        assert!(!filter.enabled("system.memory.total"));

        let prefix_filter = MetricFilter::new(vec!["system.cpu.".to_string()], vec![]);
        assert!(prefix_filter.enabled("system.cpu"));
        assert!(prefix_filter.enabled("system.cpu.utilization"));
    }

    #[test]
    fn process_label_config_defaults_to_low_cardinality() {
        let cfg = ProcessLabelConfig::default();
        assert!(!cfg.include_pid);
        assert!(cfg.include_command);
        assert!(cfg.include_state);
    }
}
