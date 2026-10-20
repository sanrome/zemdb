//! Schema evolution with constraints: a column added together with constraints that include it
//! changes no decision on the operations sequenced before it.
//!
//! A random log adds such columns at random points. Replicas that learn of each new version when
//! a batch first needs it, and replicas that apply the whole log under the latest version, must
//! report the same rejections and hold the same rows. So must a disk replica that reopens with
//! the latest version a WAL and a snapshot written under older ones, and a replica that installs,
//! under the latest version, a snapshot taken under an older one and applies the rest of the log.
//!
//! The ignored variant runs many more logs: `cargo test --release -- --ignored`.

mod common;

use common::oracle::{self, Oracle};
use common::rng::Rng;
use common::{disk_engine, reject_reasons, room_rows, trimmed_dump};
use std::collections::BTreeSet;
use std::path::Path;
use zemdb_core::{RoomId, Schema, SequencedOperation};
use zemdb_storage::{DiskStorageEngine, MemoryStorageEngine, StorageEngine};

/// Closes `room` and opens it again with `schema`.
async fn reopen(
    engine: DiskStorageEngine,
    dir: &Path,
    room: &RoomId,
    schema: Schema,
) -> DiskStorageEngine {
    engine.close_room(room).await.unwrap();
    let engine = disk_engine(dir);
    engine.open_room(room, schema).await.unwrap();
    engine
}

/// Runs one random evolving log through every replica, and returns the reasons it rejected
/// operations for.
async fn check_evolution(seed: u64) -> BTreeSet<String> {
    let case = format!("seed {seed}");
    let mut rng = Rng::new(seed.wrapping_mul(0x51_7CC1) + 7);
    let mut versions = vec![oracle::schema()];
    for n in 0..1 + rng.below(4) as usize {
        let next = oracle::add_random_column(&mut rng, versions.last().unwrap(), n);
        versions.push(next);
    }
    let latest = versions.last().unwrap().clone();
    let len = 80 + rng.below(120);
    // The first sequence of each version, in the first half of the log: the operations from
    // there on are written under it.
    let mut starts: Vec<u64> = (1..versions.len())
        .map(|_| 1 + rng.below(len / 2))
        .collect();
    starts.sort_unstable();
    starts.insert(0, 1);
    let version_at = |seq: u64| starts.iter().rposition(|&start| start <= seq).unwrap();

    let room = RoomId::new("evolving").unwrap();
    // The replicas that know each version from the first batch that needs it, and the model,
    // which decides the same way.
    let mut model = Oracle::new(versions[0].clone());
    let mut known = 0;
    let knowing_memory = MemoryStorageEngine::new();
    knowing_memory
        .open_room(&room, versions[0].clone())
        .await
        .unwrap();
    let knowing_dir = tempfile::tempdir().unwrap();
    let mut knowing_disk = disk_engine(knowing_dir.path());
    knowing_disk
        .open_room(&room, versions[0].clone())
        .await
        .unwrap();
    // The replicas that apply everything under the latest version.
    let latest_memory = MemoryStorageEngine::new();
    latest_memory
        .open_room(&room, latest.clone())
        .await
        .unwrap();
    let latest_dir = tempfile::tempdir().unwrap();
    let mut latest_disk = disk_engine(latest_dir.path());
    latest_disk.open_room(&room, latest.clone()).await.unwrap();
    // A replica that installs, under the latest version, a snapshot of the knowing disk replica
    // taken at a random point of the first half of the log, and applies the rest.
    let late_at = rng.below(len / 2);
    let mut late: Option<MemoryStorageEngine> = None;

    let mut reasons = BTreeSet::new();
    while model.head() < len {
        let first = model.head() + 1;
        let size = (1 + rng.below(8)).min(len - model.head());
        let batch: Vec<SequencedOperation> = (first..first + size)
            .map(|seq| {
                let schema = &versions[version_at(seq)];
                let op = oracle::random_operation(&mut rng, schema);
                schema
                    .validate_operation(&op)
                    .expect("a valid random operation");
                SequencedOperation::new(seq, op)
            })
            .collect();

        let needed = version_at(first + size - 1);
        if needed > known {
            known = needed;
            let schema = versions[known].clone();
            model.evolve(schema.clone());
            // The memory replica moves to the new version through a snapshot of itself, taken
            // under the old one.
            let head = knowing_memory.get_head_seq(&room).await.unwrap();
            let snapshot = knowing_memory.create_snapshot(&room).await.unwrap();
            knowing_memory.close_room(&room).await.unwrap();
            knowing_memory
                .open_room(&room, schema.clone())
                .await
                .unwrap();
            knowing_memory
                .apply_snapshot(&room, schema.clone(), snapshot.into(), head)
                .await
                .unwrap();
            knowing_disk = reopen(knowing_disk, knowing_dir.path(), &room, schema).await;
        }
        if rng.chance(10) {
            knowing_disk.compact_room(&room).await.unwrap();
        }
        if rng.chance(10) {
            latest_disk.compact_room(&room).await.unwrap();
        }
        if rng.chance(5) {
            latest_disk = reopen(latest_disk, latest_dir.path(), &room, latest.clone()).await;
        }
        if late.is_none() && model.head() >= late_at {
            let head = knowing_disk.get_head_seq(&room).await.unwrap();
            let snapshot = knowing_disk.create_snapshot(&room).await.unwrap();
            let engine = MemoryStorageEngine::new();
            engine.open_room(&room, latest.clone()).await.unwrap();
            engine
                .apply_snapshot(&room, latest.clone(), snapshot.into(), head)
                .await
                .unwrap();
            late = Some(engine);
        }

        let rejected = model.apply_batch(&batch);
        reasons.extend(rejected.iter().map(|(_, reason)| format!("{reason:?}")));
        let expected: Vec<_> = reject_reasons(rejected);
        let mut replicas: Vec<(&str, &dyn StorageEngine)> = vec![
            ("knowing memory", &knowing_memory),
            ("knowing disk", &knowing_disk),
            ("latest memory", &latest_memory),
            ("latest disk", &latest_disk),
        ];
        if let Some(late) = &late {
            replicas.push(("late", late));
        }
        for (name, engine) in replicas {
            let report = engine.apply_batch(&room, batch.clone()).await.unwrap();
            assert_eq!(
                report.rejected(),
                expected.as_slice(),
                "{case}: {name}, batch at {first}"
            );
            assert_eq!(report.head_seq().get(), model.head(), "{case}: {name}");
        }
    }
    assert_eq!(
        known,
        versions.len() - 1,
        "{case}: a version was never used"
    );

    let want = trimmed_dump(model.rows());
    let knowing_disk = reopen(knowing_disk, knowing_dir.path(), &room, latest.clone()).await;
    let latest_disk = reopen(latest_disk, latest_dir.path(), &room, latest.clone()).await;
    let late = late.expect("a late replica");
    let replicas: [(&str, &dyn StorageEngine); 5] = [
        ("knowing memory", &knowing_memory),
        ("knowing disk, reopened", &knowing_disk),
        ("latest memory", &latest_memory),
        ("latest disk, reopened", &latest_disk),
        ("late", &late),
    ];
    for (name, engine) in replicas {
        assert_eq!(
            trimmed_dump(&room_rows(engine, &room, &latest).await),
            want,
            "{case}: {name}"
        );
    }
    reasons
}

/// The reasons the random columns' own constraints rejected operations for: those whose
/// constraint is named after an added column.
fn added_column_reasons(reasons: &BTreeSet<String>) -> BTreeSet<&str> {
    reasons
        .iter()
        .filter(|reason| reason.contains("(\"e"))
        .map(|reason| &reason[..reason.find('(').unwrap()])
        .collect()
}

#[tokio::test]
async fn replicas_that_learn_each_version_at_any_point_decide_the_same() {
    let mut reasons = BTreeSet::new();
    for seed in 0..20 {
        reasons.extend(check_evolution(seed).await);
    }
    // The constraints of the added columns reject operations of every kind but deletes, which
    // need a row that references its parent through an added column: the long variant sees
    // those too.
    let added = added_column_reasons(&reasons);
    assert!(
        added.contains("Unique") && added.contains("ForeignKey"),
        "{reasons:?}"
    );
}

#[tokio::test]
#[ignore = "long-running randomized test; run with --ignored in release"]
async fn replicas_that_learn_each_version_at_any_point_decide_the_same_over_many_logs() {
    let mut reasons = BTreeSet::new();
    for seed in 0..300 {
        reasons.extend(check_evolution(1000 + seed).await);
    }
    assert_eq!(
        added_column_reasons(&reasons),
        BTreeSet::from(["ForeignKey", "Restrict", "Unique"]),
        "{reasons:?}"
    );
}
