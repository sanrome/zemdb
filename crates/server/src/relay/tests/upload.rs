use super::*;
use crate::relay::test_common::*;

#[tokio::test]
async fn failed_directory_sync_keeps_the_previous_snapshot() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    let old = stage(&relay, 5, &envelope(100, 1)).await.unwrap();

    fail_point::arm("sync_dir", &snapshots_dir(&dir));
    let result = stage(&relay, 6, &envelope(100, 2)).await;
    assert!(matches!(result, Err(ServerError::Io(_))), "{result:?}");

    assert_eq!(relay.active_snapshot_seq(&room()), Some(seq(5)));
    assert_eq!(
        files_in(&snapshots_dir(&dir)),
        vec![snapshot_file_name(&room(), seq(5), &old)]
    );
    assert!(files_in(&uploads_dir(&dir)).is_empty());
    let (_, hash, _) = chunk(&relay, 0, 0, None).await.unwrap();
    assert_eq!(hash, old);
}

#[tokio::test]
async fn snapshot_outside_the_log_range_is_rejected() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    let data = Bytes::from(envelope(100, 1));

    // Retained log 10..=20: a snapshot can sit at 9..=20.
    for n in [8, 21] {
        assert_bad_request(
            relay
                .stage_snapshot(&room(), seq(n), data.clone(), bounds(10, 20))
                .await,
        );
    }
    assert_eq!(relay.active_snapshot_seq(&room()), None);
    assert!(files_in(&snapshots_dir(&dir)).is_empty());

    relay
        .stage_snapshot(&room(), seq(9), data.clone(), bounds(10, 20))
        .await
        .unwrap();
    relay
        .stage_snapshot(&room(), seq(20), data, bounds(10, 20))
        .await
        .unwrap();
    assert_eq!(relay.active_snapshot_seq(&room()), Some(seq(20)));
}

#[tokio::test]
async fn snapshot_not_newer_than_the_active_one_is_rejected() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    let active = stage(&relay, 10, &envelope(100, 1)).await.unwrap();

    assert_superseded(stage(&relay, 10, &envelope(100, 2)).await);
    assert_superseded(stage(&relay, 9, &envelope(100, 3)).await);
    assert_eq!(relay.active_snapshot_seq(&room()), Some(seq(10)));
    assert_eq!(
        files_in(&snapshots_dir(&dir)),
        vec![snapshot_file_name(&room(), seq(10), &active)]
    );
}

#[tokio::test]
async fn snapshot_with_an_invalid_header_or_checksum_is_rejected() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);

    let mut bad_magic = envelope(100, 1);
    bad_magic[0] = b'X';
    assert_bad_request(stage(&relay, 1, &bad_magic).await);

    let mut bad_crc = envelope(100, 1);
    bad_crc[99] ^= 0xFF;
    assert_bad_request(stage(&relay, 1, &bad_crc).await);

    assert_bad_request(stage(&relay, 1, b"short").await);
    assert_eq!(relay.active_snapshot_seq(&room()), None);
    assert!(files_in(&snapshots_dir(&dir)).is_empty());
    assert!(files_in(&uploads_dir(&dir)).is_empty());
}

#[tokio::test]
async fn snapshot_above_the_size_limit_is_rejected() {
    let dir = tempdir().unwrap();
    let relay = SnapshotRelay::new(dir.path().join("snapshots"), TTL, 1000).unwrap();
    assert_bad_request(stage(&relay, 1, &envelope(1001, 1)).await);
    stage(&relay, 1, &envelope(1000, 1)).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_stages_never_move_the_active_snapshot_backwards() {
    let dir = tempdir().unwrap();
    let relay = Arc::new(open(&dir));

    let mut tasks = Vec::new();
    for n in [7u64, 3, 12, 1, 9, 15, 4, 11, 2, 14, 6, 13, 5, 10, 8] {
        let relay = Arc::clone(&relay);
        tasks.push(tokio::spawn(async move {
            // At most two single-request uploads wait per room; retry the rejected ones.
            loop {
                let result = relay
                    .stage_snapshot(
                        &room(),
                        seq(n),
                        Bytes::from(envelope(4096, n as u8)),
                        bounds(0, 100),
                    )
                    .await;
                if !matches!(result, Err(ServerError::RateLimited)) {
                    break result;
                }
                tokio::task::yield_now().await;
            }
        }));
    }
    let mut accepted_15 = None;
    for (task, n) in tasks
        .into_iter()
        .zip([7u64, 3, 12, 1, 9, 15, 4, 11, 2, 14, 6, 13, 5, 10, 8])
    {
        match task.await.unwrap() {
            Ok(hash) if n == 15 => accepted_15 = Some(hash),
            Ok(_) | Err(ServerError::SnapshotSuperseded(_)) => {}
            Err(other) => panic!("unexpected error for seq {n}: {other:?}"),
        }
    }

    // The highest sequence always wins, and only its file is left.
    let hash = accepted_15.expect("the highest sequence is never superseded");
    assert_eq!(relay.active_snapshot_seq(&room()), Some(seq(15)));
    assert_eq!(
        files_in(&snapshots_dir(&dir)),
        vec![snapshot_file_name(&room(), seq(15), &hash)]
    );
    assert!(files_in(&uploads_dir(&dir)).is_empty());
}

#[tokio::test]
async fn multipart_upload_out_of_order_assembles_and_stages() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    let (data, layout, hash, ranges) = three_part_snapshot(7);

    for index in [2u32, 0] {
        let r = ranges[index as usize].clone();
        assert!(!relay
            .stage_chunk(part(10, index, layout, hash, &data[r]), bounds(0, 10))
            .await
            .unwrap());
        assert_eq!(relay.active_snapshot_seq(&room()), None);
    }
    assert_eq!(files_in(&uploads_dir(&dir)).len(), 1);

    assert!(relay
        .stage_chunk(
            part(10, 1, layout, hash, &data[ranges[1].clone()]),
            bounds(0, 10)
        )
        .await
        .unwrap());
    assert_eq!(relay.active_snapshot_seq(&room()), Some(seq(10)));
    assert!(files_in(&uploads_dir(&dir)).is_empty());

    let mut assembled = Vec::new();
    let (total, _, first) = chunk(&relay, 0, 0, Some(hash)).await.unwrap();
    assembled.extend_from_slice(&first);
    for index in 1..total {
        let (_, _, next) = chunk(&relay, index, 0, Some(hash)).await.unwrap();
        assembled.extend_from_slice(&next);
    }
    assert_eq!(assembled, data);
}

#[tokio::test]
async fn second_upload_for_a_lower_or_equal_seq_is_rejected() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    let (data, layout, hash, ranges) = three_part_snapshot(1);
    relay
        .stage_chunk(
            part(10, 0, layout, hash, &data[ranges[0].clone()]),
            bounds(0, 20),
        )
        .await
        .unwrap();

    let (other, other_layout, other_hash, other_ranges) = three_part_snapshot(2);
    assert_superseded(
        relay
            .stage_chunk(
                SnapshotChunkUpload {
                    uploader: member("other"),
                    ..part(
                        9,
                        0,
                        other_layout,
                        other_hash,
                        &other[other_ranges[0].clone()],
                    )
                },
                bounds(0, 20),
            )
            .await,
    );
    assert_superseded(
        relay
            .stage_chunk(
                SnapshotChunkUpload {
                    uploader: member("other"),
                    ..part(
                        10,
                        0,
                        other_layout,
                        other_hash,
                        &other[other_ranges[0].clone()],
                    )
                },
                bounds(0, 20),
            )
            .await,
    );

    // The original upload is untouched and still completes.
    for index in [1u32, 2] {
        relay
            .stage_chunk(
                part(
                    10,
                    index,
                    layout,
                    hash,
                    &data[ranges[index as usize].clone()],
                ),
                bounds(0, 20),
            )
            .await
            .unwrap();
    }
    assert_eq!(relay.active_snapshot_seq(&room()), Some(seq(10)));
}

#[tokio::test]
async fn upload_for_a_higher_seq_replaces_the_current_one_and_deletes_its_file() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    let (data, layout, hash, ranges) = three_part_snapshot(1);
    relay
        .stage_chunk(
            part(10, 0, layout, hash, &data[ranges[0].clone()]),
            bounds(0, 20),
        )
        .await
        .unwrap();
    assert_eq!(files_in(&uploads_dir(&dir)), vec!["relay-room_10.part"]);

    let (newer, newer_layout, newer_hash, newer_ranges) = three_part_snapshot(2);
    relay
        .stage_chunk(
            part(
                11,
                0,
                newer_layout,
                newer_hash,
                &newer[newer_ranges[0].clone()],
            ),
            bounds(0, 20),
        )
        .await
        .unwrap();
    assert_eq!(files_in(&uploads_dir(&dir)), vec!["relay-room_11.part"]);

    // The replaced upload is gone: its next chunk would start a new upload at a lower seq.
    assert_superseded(
        relay
            .stage_chunk(
                part(10, 1, layout, hash, &data[ranges[1].clone()]),
                bounds(0, 20),
            )
            .await,
    );
}

#[tokio::test]
async fn upload_sizes_are_bounded() {
    let dir = tempdir().unwrap();
    let relay = SnapshotRelay::new(dir.path().join("snapshots"), TTL, 8 * 1024 * 1024).unwrap();
    let hash = [0u8; 32];
    let chunk_of = |len: usize| vec![0u8; len];

    // Total above the relay's maximum snapshot size.
    assert_bad_request(
        relay
            .stage_chunk(
                part(1, 0, (8 * 1024 * 1024 + 1, 3), hash, &chunk_of(2_796_203)),
                bounds(0, 10),
            )
            .await,
    );
    // Chunks above 4 MiB.
    assert_bad_request(
        relay
            .stage_chunk(
                part(1, 0, (8 * 1024 * 1024, 1), hash, &chunk_of(8 * 1024 * 1024)),
                bounds(0, 10),
            )
            .await,
    );
    // Chunks below 64 KiB when there is more than one.
    assert_bad_request(
        relay
            .stage_chunk(part(1, 0, (1000, 2), hash, &chunk_of(500)), bounds(0, 10))
            .await,
    );
    // Out-of-range index and zero chunks.
    assert_bad_request(
        relay
            .stage_chunk(
                part(1, 3, (200 * 1024, 3), hash, &chunk_of(68266)),
                bounds(0, 10),
            )
            .await,
    );
    assert_bad_request(
        relay
            .stage_chunk(
                part(1, 0, (200 * 1024, 0), hash, &chunk_of(68266)),
                bounds(0, 10),
            )
            .await,
    );
    assert!(files_in(&uploads_dir(&dir)).is_empty());
}

#[tokio::test]
async fn chunk_of_the_wrong_size_is_rejected() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    let (data, layout, hash, ranges) = three_part_snapshot(1);

    // A middle chunk must be exactly ceil(total / chunks) bytes.
    assert_bad_request(
        relay
            .stage_chunk(part(10, 0, layout, hash, &data[0..1000]), bounds(0, 10))
            .await,
    );
    let mut long = data[ranges[1].clone()].to_vec();
    long.push(0);
    assert_bad_request(
        relay
            .stage_chunk(part(10, 1, layout, hash, &long), bounds(0, 10))
            .await,
    );
    // The last chunk must be exactly the remainder.
    let mut last = data[ranges[2].clone()].to_vec();
    last.pop();
    assert_bad_request(
        relay
            .stage_chunk(part(10, 2, layout, hash, &last), bounds(0, 10))
            .await,
    );
    assert!(files_in(&uploads_dir(&dir)).is_empty());
}

#[tokio::test]
async fn multipart_upload_with_a_wrong_hash_is_rejected_and_discarded() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    let (data, layout, _, ranges) = three_part_snapshot(1);
    let wrong_hash = [0xEE; 32];
    for index in [0u32, 1] {
        relay
            .stage_chunk(
                part(
                    10,
                    index,
                    layout,
                    wrong_hash,
                    &data[ranges[index as usize].clone()],
                ),
                bounds(0, 10),
            )
            .await
            .unwrap();
    }
    let result = relay
        .stage_chunk(
            part(10, 2, layout, wrong_hash, &data[ranges[2].clone()]),
            bounds(0, 10),
        )
        .await;
    match result {
        Err(ServerError::BadRequest(msg)) => assert!(msg.contains("BLAKE3"), "{msg}"),
        other => panic!("expected BadRequest, got {other:?}"),
    }
    assert_eq!(relay.active_snapshot_seq(&room()), None);
    assert!(files_in(&uploads_dir(&dir)).is_empty());
    assert!(files_in(&snapshots_dir(&dir)).is_empty());
}

#[tokio::test]
async fn multipart_upload_with_a_corrupted_envelope_is_rejected() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    let (mut data, layout, _, ranges) = three_part_snapshot(1);
    data[100_000] ^= 0xFF;
    let hash = ServerMessage::compute_snapshot_hash(&data);
    for index in [0u32, 1] {
        relay
            .stage_chunk(
                part(
                    10,
                    index,
                    layout,
                    hash,
                    &data[ranges[index as usize].clone()],
                ),
                bounds(0, 10),
            )
            .await
            .unwrap();
    }
    let result = relay
        .stage_chunk(
            part(10, 2, layout, hash, &data[ranges[2].clone()]),
            bounds(0, 10),
        )
        .await;
    match result {
        Err(ServerError::BadRequest(msg)) => assert!(msg.contains("CRC32"), "{msg}"),
        other => panic!("expected BadRequest, got {other:?}"),
    }
    assert!(files_in(&uploads_dir(&dir)).is_empty());
    assert!(files_in(&snapshots_dir(&dir)).is_empty());
}

#[tokio::test]
async fn multipart_completion_rechecks_the_log_range() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    let (data, layout, hash, ranges) = three_part_snapshot(1);
    for index in [0u32, 1] {
        relay
            .stage_chunk(
                part(
                    10,
                    index,
                    layout,
                    hash,
                    &data[ranges[index as usize].clone()],
                ),
                bounds(5, 10),
            )
            .await
            .unwrap();
    }
    // The log was compacted meanwhile: seq 10 is now below tail - 1.
    assert_bad_request(
        relay
            .stage_chunk(
                part(10, 2, layout, hash, &data[ranges[2].clone()]),
                bounds(12, 30),
            )
            .await,
    );
    assert_eq!(relay.active_snapshot_seq(&room()), None);
    assert!(files_in(&uploads_dir(&dir)).is_empty());
}

#[tokio::test]
async fn newer_snapshot_discards_the_upload_in_progress() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    let (data, layout, hash, ranges) = three_part_snapshot(1);
    for index in [0u32, 1] {
        relay
            .stage_chunk(
                part(
                    10,
                    index,
                    layout,
                    hash,
                    &data[ranges[index as usize].clone()],
                ),
                bounds(0, 20),
            )
            .await
            .unwrap();
    }
    // A newer snapshot arrives in one request; it makes the upload in progress obsolete.
    stage(&relay, 12, &envelope(100, 9)).await.unwrap();
    assert!(files_in(&uploads_dir(&dir)).is_empty());
    assert_superseded(
        relay
            .stage_chunk(
                part(10, 2, layout, hash, &data[ranges[2].clone()]),
                bounds(0, 20),
            )
            .await,
    );
    assert_eq!(relay.active_snapshot_seq(&room()), Some(seq(12)));
}

#[tokio::test]
async fn resending_a_chunk_of_the_installed_upload_is_acknowledged_as_staged() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    let (data, layout, hash, ranges) = three_part_snapshot(1);
    for index in [0u32, 1, 2] {
        relay
            .stage_chunk(
                part(
                    10,
                    index,
                    layout,
                    hash,
                    &data[ranges[index as usize].clone()],
                ),
                bounds(0, 10),
            )
            .await
            .unwrap();
    }
    // The acknowledgment of the last chunk was lost; the client sends it again.
    assert!(relay
        .stage_chunk(
            part(10, 2, layout, hash, &data[ranges[2].clone()]),
            bounds(0, 10)
        )
        .await
        .unwrap());
    assert!(files_in(&uploads_dir(&dir)).is_empty());
}

#[tokio::test]
async fn single_request_uploads_waiting_per_room_are_bounded() {
    let dir = tempdir().unwrap();
    let relay = Arc::new(open(&dir));
    let mut gates = Vec::new();
    let mut waiting = Vec::new();
    for n in [1u64, 2] {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        gates.push(tx);
        let relay = Arc::clone(&relay);
        waiting.push(tokio::spawn(async move {
            relay
                .stage_snapshot(
                    &room(),
                    seq(n),
                    Bytes::from(envelope(100, n as u8)),
                    async {
                        rx.await.ok();
                        Ok(LogBounds {
                            tail_seq: seq(0),
                            head_seq: seq(10),
                        })
                    },
                )
                .await
        }));
    }
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }

    let third = tokio::time::timeout(
        Duration::from_secs(5),
        relay.stage_snapshot(
            &room(),
            seq(3),
            Bytes::from(envelope(100, 3)),
            bounds(0, 10),
        ),
    )
    .await
    .expect("a third upload must be rejected, not wait");
    assert!(matches!(third, Err(ServerError::RateLimited)), "{third:?}");

    // Other rooms are not affected, and the limit frees up once uploads finish.
    let other = RoomId::new("other-room").unwrap();
    relay
        .stage_snapshot(&other, seq(1), Bytes::from(envelope(100, 1)), bounds(0, 10))
        .await
        .unwrap();
    drop(gates);
    for task in waiting {
        let _ = task.await.unwrap();
    }
    stage(&relay, 4, &envelope(100, 4)).await.unwrap();
}

#[tokio::test]
async fn snapshot_worker_preempts_a_member_upload_at_the_same_seq() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    // A member keeps a bogus upload at seq 10 alive.
    let mut bogus = three_part_snapshot(1);
    bogus.2 = [0xEE; 32];
    relay
        .stage_chunk(part_by(member("mallory"), 10, 0, &bogus), bounds(0, 10))
        .await
        .unwrap();

    // Another member cannot replace it at the same seq...
    let honest = three_part_snapshot(2);
    assert_superseded(
        relay
            .stage_chunk(part_by(member("alice"), 10, 0, &honest), bounds(0, 10))
            .await,
    );

    // ...but the snapshot worker can, and completes its upload.
    for index in 0..3 {
        relay
            .stage_chunk(part_by(Uploader::Admin, 10, index, &honest), bounds(0, 10))
            .await
            .unwrap();
    }
    assert_eq!(relay.active_snapshot_seq(&room()), Some(seq(10)));
    assert!(files_in(&uploads_dir(&dir)).is_empty());

    // The member's next chunk does not disturb anything.
    assert_superseded(
        relay
            .stage_chunk(part_by(member("mallory"), 10, 1, &bogus), bounds(0, 10))
            .await,
    );
}

#[tokio::test]
async fn snapshot_worker_does_not_preempt_a_newer_upload() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    let newer = three_part_snapshot(1);
    relay
        .stage_chunk(part_by(member("alice"), 11, 0, &newer), bounds(0, 20))
        .await
        .unwrap();
    assert_superseded(
        relay
            .stage_chunk(
                part_by(Uploader::Admin, 10, 0, &three_part_snapshot(2)),
                bounds(0, 20),
            )
            .await,
    );
}

#[tokio::test]
async fn uploader_may_restart_its_own_upload_at_the_same_seq() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    let first = three_part_snapshot(1);
    relay
        .stage_chunk(part_by(member("alice"), 10, 0, &first), bounds(0, 10))
        .await
        .unwrap();

    let second = three_part_snapshot(2);
    for index in 0..3 {
        relay
            .stage_chunk(part_by(member("alice"), 10, index, &second), bounds(0, 10))
            .await
            .unwrap();
    }
    let (_, hash, _) = chunk(&relay, 0, 0, None).await.unwrap();
    assert_eq!(hash, second.2);
}

#[tokio::test(start_paused = true)]
async fn multipart_upload_is_in_progress_until_it_completes_or_goes_idle() {
    let dir = tempdir().unwrap();
    let relay = open(&dir);
    let (data, layout, hash, ranges) = three_part_snapshot(1);
    assert!(!relay.has_upload_in_progress(&room()));

    for index in 0..2u32 {
        relay
            .stage_chunk(
                part(
                    10,
                    index,
                    layout,
                    hash,
                    &data[ranges[index as usize].clone()],
                ),
                bounds(0, 10),
            )
            .await
            .unwrap();
        assert!(relay.has_upload_in_progress(&room()));
    }
    assert!(!relay.has_upload_in_progress(&RoomId::new("other-room").unwrap()));

    relay
        .stage_chunk(
            part(10, 2, layout, hash, &data[ranges[2].clone()]),
            bounds(0, 10),
        )
        .await
        .unwrap();
    assert!(!relay.has_upload_in_progress(&room()));

    // An upload that stopped receiving chunks no longer counts.
    let (data, layout, hash, ranges) = three_part_snapshot(2);
    relay
        .stage_chunk(
            part(11, 0, layout, hash, &data[ranges[0].clone()]),
            bounds(0, 11),
        )
        .await
        .unwrap();
    assert!(relay.has_upload_in_progress(&room()));
    tokio::time::advance(crate::relay::UPLOAD_IDLE_TIMEOUT).await;
    assert!(!relay.has_upload_in_progress(&room()));
}

#[tokio::test]
async fn single_request_upload_is_in_progress_while_it_is_handled() {
    let dir = tempdir().unwrap();
    let relay = Arc::new(open(&dir));
    let (gate, gate_rx) = tokio::sync::oneshot::channel::<()>();
    let upload = tokio::spawn({
        let relay = Arc::clone(&relay);
        async move {
            relay
                .stage_snapshot(&room(), seq(5), Bytes::from(envelope(100, 1)), async {
                    gate_rx.await.ok();
                    Ok(LogBounds {
                        tail_seq: seq(0),
                        head_seq: seq(10),
                    })
                })
                .await
        }
    });
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert!(relay.has_upload_in_progress(&room()));

    gate.send(()).unwrap();
    upload.await.unwrap().unwrap();
    assert!(!relay.has_upload_in_progress(&room()));
}
