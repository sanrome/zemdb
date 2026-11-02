use super::*;
use crate::relay::test_common::*;

#[test]
fn snapshot_file_names_roundtrip_with_underscores_in_room_ids() {
    let room_id = RoomId::new("a_b_c").unwrap();
    let hash = [0xAB; 32];
    let name = snapshot_file_name(&room_id, seq(77), &hash);
    assert_eq!(
        parse_snapshot_file_name(&name),
        Some((room_id.clone(), seq(77), hash))
    );
    let upload = upload_file_name(&room_id, seq(5), PART_EXTENSION);
    assert_eq!(parse_upload_file_name(&upload), Some((room_id, seq(5))));
    assert_eq!(parse_snapshot_file_name("a_b_c_77.snap"), None);
}

/// The relay runs at most its share of the server's blocking threads at once, so that it never
/// takes a thread an operation of the rooms needs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_relay_runs_at_most_its_blocking_operations_at_once() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    let (running, peak) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let operations: Vec<_> = (0..3 * crate::io_pool::RELAY_THREADS)
        .map(|_| {
            let (running, peak) = (Arc::clone(&running), Arc::clone(&peak));
            tokio::spawn(run_blocking(move || {
                let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                std::thread::sleep(std::time::Duration::from_millis(20));
                running.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            }))
        })
        .collect();
    for operation in operations {
        operation.await.unwrap().unwrap();
    }
    assert!(peak.load(Ordering::SeqCst) <= crate::io_pool::RELAY_THREADS);
}
