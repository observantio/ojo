    use super::{IntervalBuffer, OFFLINE_BUFFER_INTERVALS};

    #[test]
    fn interval_buffer_is_fifo() {
        let mut buf = IntervalBuffer::new(OFFLINE_BUFFER_INTERVALS);
        for value in [1, 2, 3] {
            assert!(!buf.push(value));
        }

        assert_eq!(buf.pop(), Some(1));
        assert_eq!(buf.pop(), Some(2));
        assert_eq!(buf.pop(), Some(3));
        assert_eq!(buf.pop(), None);
    }

    #[test]
    fn interval_buffer_caps_and_drops_oldest() {
        let mut buf = IntervalBuffer::new(OFFLINE_BUFFER_INTERVALS);
        for value in 0..OFFLINE_BUFFER_INTERVALS {
            assert!(!buf.push(value));
        }

        assert!(buf.push(99));
        assert_eq!(buf.len(), OFFLINE_BUFFER_INTERVALS);
        assert_eq!(buf.dropped_intervals(), 1);
        assert_eq!(buf.pop(), Some(1));
    }
