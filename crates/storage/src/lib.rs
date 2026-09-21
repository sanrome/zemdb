pub mod engine;
pub mod error;
pub mod memory;
pub mod options;

pub use engine::{EngineConcurrencyBounds, RowStream, StorageEngine};
pub use error::StorageError;
pub use memory::MemoryStorageEngine;
pub use options::{KeyRange, ScanDirection, ScanOptions};

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use rimdb_core::{
        ColumnUpdate, CompactRow, DataType, Operation, PrimaryKey, RoomId, Schema, SequenceNumber,
        SequencedOperation, TableSchema, Value,
    };

    fn test_schema() -> Schema {
        let users = TableSchema::builder("users")
            .primary_key("id", DataType::Int)
            .column("name", DataType::String)
            .column("score", DataType::Int)
            .column("active", DataType::Bool)
            .build()
            .expect("valid users table");

        Schema::builder().table(users).build()
    }

    #[tokio::test]
    async fn test_room_lifecycle() {
        let engine = MemoryStorageEngine::new();
        let room_id = RoomId::new("room-1");
        let schema = test_schema();

        // 1. Open room
        assert!(engine.open_room(&room_id, schema.clone()).await.is_ok());

        // 2. Open again should fail with RoomAlreadyOpen
        let err = engine.open_room(&room_id, schema).await.unwrap_err();
        assert!(matches!(err, StorageError::RoomAlreadyOpen(_)));

        // 3. Head seq should initially be 0
        let head = engine.get_head_seq(&room_id).await.unwrap();
        assert_eq!(head, SequenceNumber::from(0u64));

        // 4. Close room
        assert!(engine.close_room(&room_id).await.is_ok());

        // 5. Querying closed room should fail with RoomNotFound
        let err = engine
            .get(&room_id, "users", &PrimaryKey::single(1i64))
            .await
            .unwrap_err();
        assert!(matches!(err, StorageError::RoomNotFound(_)));
    }

    #[tokio::test]
    async fn test_point_lookup_and_batch_mutation() {
        let engine = MemoryStorageEngine::new();
        let room_id = RoomId::new("room-point");
        engine.open_room(&room_id, test_schema()).await.unwrap();

        let row1 = CompactRow::new(vec![
            Value::Int(1),
            Value::String("Alice".into()),
            Value::Int(100),
            Value::Bool(true),
        ]);
        let row2 = CompactRow::new(vec![
            Value::Int(2),
            Value::String("Bob".into()),
            Value::Int(80),
            Value::Bool(false),
        ]);

        let ops = vec![
            SequencedOperation {
                seq: SequenceNumber::from(1u64),
                op: Operation::insert("users", PrimaryKey::single(1i64), row1.clone(), 10),
            },
            SequencedOperation {
                seq: SequenceNumber::from(2u64),
                op: Operation::insert("users", PrimaryKey::single(2i64), row2.clone(), 20),
            },
        ];

        let head = engine.apply_batch(&room_id, ops).await.unwrap();
        assert_eq!(head, SequenceNumber::from(2u64));

        // Point lookups
        let res1 = engine
            .get(&room_id, "users", &PrimaryKey::single(1i64))
            .await
            .unwrap();
        assert_eq!(res1, Some(row1));

        let res2 = engine
            .get(&room_id, "users", &PrimaryKey::single(2i64))
            .await
            .unwrap();
        assert_eq!(res2, Some(row2));

        let res3 = engine
            .get(&room_id, "users", &PrimaryKey::single(999i64))
            .await
            .unwrap();
        assert_eq!(res3, None);

        // Update Alice's score to 120 (column_idx 2)
        let update_op = vec![SequencedOperation {
            seq: SequenceNumber::from(3u64),
            op: Operation::update(
                "users",
                PrimaryKey::single(1i64),
                vec![ColumnUpdate::new(2, Value::Int(120))],
                30,
            ),
        }];
        engine.apply_batch(&room_id, update_op).await.unwrap();

        let updated_alice = engine
            .get(&room_id, "users", &PrimaryKey::single(1i64))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(updated_alice.values[2], Value::Int(120));

        // Delete Bob
        let delete_op = vec![SequencedOperation {
            seq: SequenceNumber::from(4u64),
            op: Operation::delete("users", PrimaryKey::single(2i64), 40),
        }];
        engine.apply_batch(&room_id, delete_op).await.unwrap();

        let bob_after_del = engine
            .get(&room_id, "users", &PrimaryKey::single(2i64))
            .await
            .unwrap();
        assert_eq!(bob_after_del, None);
    }

    #[tokio::test]
    async fn test_range_scans_and_reverse_ordering() {
        let engine = MemoryStorageEngine::new();
        let room_id = RoomId::new("room-scan");
        engine.open_room(&room_id, test_schema()).await.unwrap();

        let mut ops = Vec::new();
        for i in 1..=5i64 {
            let row = CompactRow::new(vec![
                Value::Int(i),
                Value::String(format!("User {i}").into()),
                Value::Int(i * 10),
                Value::Bool(true),
            ]);
            ops.push(SequencedOperation {
                seq: SequenceNumber::from(i as u64),
                op: Operation::insert("users", PrimaryKey::single(i), row, 100),
            });
        }
        engine.apply_batch(&room_id, ops).await.unwrap();

        // 1. Forward full scan
        let mut stream = engine
            .scan(&room_id, "users", ScanOptions::new())
            .await
            .unwrap();
        let mut pks = Vec::new();
        while let Some(item) = stream.next().await {
            let (pk, _) = item.unwrap();
            pks.push(pk);
        }
        assert_eq!(
            pks,
            vec![
                PrimaryKey::single(1i64),
                PrimaryKey::single(2i64),
                PrimaryKey::single(3i64),
                PrimaryKey::single(4i64),
                PrimaryKey::single(5i64),
            ]
        );

        // 2. Backward scan (ORDER BY pk DESC)
        let mut desc_stream = engine
            .scan(&room_id, "users", ScanOptions::new().backward())
            .await
            .unwrap();
        let mut desc_pks = Vec::new();
        while let Some(item) = desc_stream.next().await {
            let (pk, _) = item.unwrap();
            desc_pks.push(pk);
        }
        assert_eq!(
            desc_pks,
            vec![
                PrimaryKey::single(5i64),
                PrimaryKey::single(4i64),
                PrimaryKey::single(3i64),
                PrimaryKey::single(2i64),
                PrimaryKey::single(1i64),
            ]
        );

        // 3. Sub-range scan: [2, 4]
        let range = KeyRange::from(PrimaryKey::single(2i64)..=PrimaryKey::single(4i64));
        let mut range_stream = engine
            .scan(&room_id, "users", ScanOptions::new().range(range))
            .await
            .unwrap();
        let mut range_pks = Vec::new();
        while let Some(item) = range_stream.next().await {
            let (pk, _) = item.unwrap();
            range_pks.push(pk);
        }
        assert_eq!(
            range_pks,
            vec![
                PrimaryKey::single(2i64),
                PrimaryKey::single(3i64),
                PrimaryKey::single(4i64),
            ]
        );
    }

    #[tokio::test]
    async fn test_limit_and_projection_pushdowns() {
        let engine = MemoryStorageEngine::new();
        let room_id = RoomId::new("room-pushdowns");
        engine.open_room(&room_id, test_schema()).await.unwrap();

        let mut ops = Vec::new();
        for i in 1..=10i64 {
            let row = CompactRow::new(vec![
                Value::Int(i),
                Value::String(format!("Name_{i}").into()),
                Value::Int(i * 100),
                Value::Bool(i % 2 == 0),
            ]);
            ops.push(SequencedOperation {
                seq: SequenceNumber::from(i as u64),
                op: Operation::insert("users", PrimaryKey::single(i), row, 100),
            });
        }
        engine.apply_batch(&room_id, ops).await.unwrap();

        // 1. Limit pushdown: limit 3
        let mut limit_stream = engine
            .scan(&room_id, "users", ScanOptions::new().limit(3))
            .await
            .unwrap();
        let mut count = 0;
        while let Some(item) = limit_stream.next().await {
            item.unwrap();
            count += 1;
        }
        assert_eq!(count, 3);

        // 2. Limit pushdown + Backward: top 2 descending
        let mut desc_limit_stream = engine
            .scan(&room_id, "users", ScanOptions::new().backward().limit(2))
            .await
            .unwrap();
        let mut desc_limit_pks = Vec::new();
        while let Some(item) = desc_limit_stream.next().await {
            let (pk, _) = item.unwrap();
            desc_limit_pks.push(pk);
        }
        assert_eq!(
            desc_limit_pks,
            vec![PrimaryKey::single(10i64), PrimaryKey::single(9i64)]
        );

        // 3. Projection pushdown: only project column 1 (name) and column 3 (active)
        let mut proj_stream = engine
            .scan(
                &room_id,
                "users",
                ScanOptions::new().limit(1).projection(vec![1, 3]),
            )
            .await
            .unwrap();
        let (pk, proj_row) = proj_stream.next().await.unwrap().unwrap();
        assert_eq!(pk, PrimaryKey::single(1i64));
        assert_eq!(proj_row.len(), 2);
        assert_eq!(proj_row.values[0], Value::String("Name_1".into()));
        assert_eq!(proj_row.values[1], Value::Bool(false));
    }

    #[tokio::test]
    async fn test_snapshot_create_and_restore() {
        let engine_a = MemoryStorageEngine::new();
        let room_id = RoomId::new("room-snap");
        let schema = test_schema();
        engine_a.open_room(&room_id, schema.clone()).await.unwrap();

        let row = CompactRow::new(vec![
            Value::Int(42),
            Value::String("Snapshot User".into()),
            Value::Int(999),
            Value::Bool(true),
        ]);

        let ops = vec![SequencedOperation {
            seq: SequenceNumber::from(55u64),
            op: Operation::insert("users", PrimaryKey::single(42i64), row.clone(), 100),
        }];
        engine_a.apply_batch(&room_id, ops).await.unwrap();

        // Create snapshot
        let snapshot_bytes = engine_a.create_snapshot(&room_id).await.unwrap();
        assert!(!snapshot_bytes.is_empty());

        // Restore snapshot into fresh engine_b
        let engine_b = MemoryStorageEngine::new();
        let restored_head = engine_b
            .apply_snapshot(&room_id, schema, &snapshot_bytes)
            .await
            .unwrap();
        assert_eq!(restored_head, SequenceNumber::from(55u64));

        let restored_row = engine_b
            .get(&room_id, "users", &PrimaryKey::single(42i64))
            .await
            .unwrap();
        assert_eq!(restored_row, Some(row));

        let head_b = engine_b.get_head_seq(&room_id).await.unwrap();
        assert_eq!(head_b, SequenceNumber::from(55u64));
    }

    #[tokio::test]
    async fn test_concurrent_rooms_isolation() {
        let engine = std::sync::Arc::new(MemoryStorageEngine::new());
        let room_a = RoomId::new("room-a");
        let room_b = RoomId::new("room-b");
        let schema = test_schema();

        engine.open_room(&room_a, schema.clone()).await.unwrap();
        engine.open_room(&room_b, schema).await.unwrap();

        // Spawn parallel writers to room A and room B
        let engine_a = engine.clone();
        let ra = room_a.clone();
        let handle_a = tokio::spawn(async move {
            for i in 1..=20i64 {
                let row = CompactRow::new(vec![
                    Value::Int(i),
                    Value::String(format!("User A {i}").into()),
                    Value::Int(i * 10),
                    Value::Bool(true),
                ]);
                let ops = vec![SequencedOperation {
                    seq: SequenceNumber::from(i as u64),
                    op: Operation::insert("users", PrimaryKey::single(i), row, 100),
                }];
                engine_a.apply_batch(&ra, ops).await.unwrap();
            }
        });

        let engine_b = engine.clone();
        let rb = room_b.clone();
        let handle_b = tokio::spawn(async move {
            for i in 1..=20i64 {
                let row = CompactRow::new(vec![
                    Value::Int(i),
                    Value::String(format!("User B {i}").into()),
                    Value::Int(i * 20),
                    Value::Bool(false),
                ]);
                let ops = vec![SequencedOperation {
                    seq: SequenceNumber::from(i as u64),
                    op: Operation::insert("users", PrimaryKey::single(i), row, 100),
                }];
                engine_b.apply_batch(&rb, ops).await.unwrap();
            }
        });

        // Concurrently read while writing
        let engine_reader = engine.clone();
        let ra_reader = room_a.clone();
        let handle_reader = tokio::spawn(async move {
            let mut reads = 0;
            for _ in 0..10 {
                let _ = engine_reader.get(&ra_reader, "users", &PrimaryKey::single(1i64)).await;
                reads += 1;
                tokio::task::yield_now().await;
            }
            reads
        });

        handle_a.await.unwrap();
        handle_b.await.unwrap();
        let reads = handle_reader.await.unwrap();
        assert_eq!(reads, 10);

        // Verify independent state
        assert_eq!(engine.get_head_seq(&room_a).await.unwrap(), SequenceNumber::from(20u64));
        assert_eq!(engine.get_head_seq(&room_b).await.unwrap(), SequenceNumber::from(20u64));

        let user_a1 = engine.get(&room_a, "users", &PrimaryKey::single(1i64)).await.unwrap().unwrap();
        let user_b1 = engine.get(&room_b, "users", &PrimaryKey::single(1i64)).await.unwrap().unwrap();
        assert_eq!(user_a1.values[1], Value::String("User A 1".into()));
        assert_eq!(user_b1.values[1], Value::String("User B 1".into()));
    }
}

