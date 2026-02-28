use super::*;
use crate::tapir::LeaderRecordDelta;
use crate::tapirstore::TapirStore;

fn make_delta(from: u64, to: u64) -> LeaderRecordDelta<String, String> {
    LeaderRecordDelta {
        from_view: from,
        to_view: to,
        changes: vec![],
    }
}

#[test]
fn record_and_query_cdc_deltas() {
    let (_dir, mut store) = new_store();

    assert!(store.cdc_max_view().is_none());
    assert!(store.cdc_deltas_from(0).is_empty());

    store.record_cdc_delta(0, make_delta(0, 1));
    store.record_cdc_delta(1, make_delta(1, 2));
    store.record_cdc_delta(3, make_delta(3, 4));

    assert_eq!(store.cdc_max_view(), Some(3));

    let all = store.cdc_deltas_from(0);
    assert_eq!(all.len(), 3);
    assert_eq!(all[0].from_view, 0);
    assert_eq!(all[1].from_view, 1);
    assert_eq!(all[2].from_view, 3);
}

#[test]
fn cdc_deltas_from_filters_by_view() {
    let (_dir, mut store) = new_store();

    store.record_cdc_delta(0, make_delta(0, 1));
    store.record_cdc_delta(1, make_delta(1, 2));
    store.record_cdc_delta(3, make_delta(3, 4));

    // from_view=2 should skip view 0 and 1, return only view 3.
    let deltas = store.cdc_deltas_from(2);
    assert_eq!(deltas.len(), 1);
    assert_eq!(deltas[0].from_view, 3);

    // from_view=4 should return nothing.
    assert!(store.cdc_deltas_from(4).is_empty());
}
