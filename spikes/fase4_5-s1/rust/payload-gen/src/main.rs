//! `payload-gen gen <out_dir> [rows]`: a `DiskStorageEngine` room with one table of `rows`
//! rows (756,000 by default: an `Int` key, two `String`s, a `Float` and an `Int`), its
//! snapshot (`envelope_native.zmsn`, native zstd level 3) and the snapshot's uncompressed
//! payload (`payload.bin`).
//!
//! `payload-gen verify <payload.bin> <envelope>...`: decodes each envelope with the native
//! codec (zstd) and compares the result with the payload, byte for byte.

use std::time::Instant;
use zemdb_core::*;
use zemdb_storage::snapshot::{decode_snapshot_envelope, encode_snapshot_envelope};
use zemdb_storage::{DiskStorageEngine, DiskStorageOptions, StorageEngine};

const WORDS: &[&str] = &[
    "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india",
    "juliet", "kilo", "lima", "mike", "november", "oscar", "papa", "quebec", "romeo",
    "sierra", "tango", "uniform", "victor", "whiskey", "xray", "yankee", "zulu",
];

fn rng(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn row(id: i64, state: &mut u64) -> CompactRow {
    // Repetitive, as a real table is: names from a small vocabulary, a status line from a few
    // templates, scores and counts with few distinct values.
    let r = rng(state);
    let vocab: u64 = 4;
    let name = format!(
        "{} {}",
        WORDS[(r % vocab) as usize],
        WORDS[((r >> 8) % vocab) as usize]
    );
    let note_len: usize = 38;
    let mut note = format!("order {} shipped to warehouse {} ", id / 10, (r >> 16) % 8);
    while note.len() < note_len {
        note.push_str("status ok; ");
    }
    note.truncate(note_len);
    let nvals: u64 = 16;
    let score = ((r >> 24) % nvals) as f64 / 4.0;
    let count = ((r >> 40) % 50) as i64;
    CompactRow::new(vec![
        Value::Int(id),
        Value::String(name.into()),
        Value::String(note.into()),
        Value::Float(score),
        Value::Int(count),
    ])
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("gen") => gen(&args[2], args.get(3).map(|s| s.parse().unwrap()).unwrap_or(756_000)).await,
        Some("verify") => verify(&args[2], &args[3..]),
        _ => {
            eprintln!("usage: payload-gen gen <out_dir> [rows] | verify <payload> <envelope>...");
            std::process::exit(2);
        }
    }
}

async fn gen(out_dir: &str, rows: i64) {
    let table = TableSchema::builder("items")
        .table_id(1)
        .primary_key("id", DataType::Int)
        .column("email", DataType::String)
        .column("note", DataType::String)
        .column("score", DataType::Float)
        .column("count", DataType::Int);
    let schema = Schema::builder().table(table).build();
    let tmp = tempfile::tempdir().unwrap();
    let engine =
        DiskStorageEngine::new(DiskStorageOptions::new(tmp.path()).auto_compact(false)).unwrap();
    let room = RoomId::new("payload").unwrap();
    engine.open_room(&room, schema).await.unwrap();
    let mut state = 0x2545_F491_4F6C_DD1Du64;
    let mut seq = 0u64;
    let t0 = Instant::now();
    for chunk_start in (1..=rows).step_by(10_000) {
        let ops: Vec<SequencedOperation> = (chunk_start..(chunk_start + 10_000).min(rows + 1))
            .map(|id| {
                seq += 1;
                SequencedOperation::new(
                    seq,
                    Operation::insert(1, PrimaryKey::single(id), row(id, &mut state), 1_000 + id as u64),
                )
            })
            .collect();
        engine.apply_batch(&room, ops).await.unwrap();
    }
    let t_fill = t0.elapsed();
    let t1 = Instant::now();
    let envelope = engine.create_snapshot(&room).await.unwrap();
    let t_snap = t1.elapsed();
    let t2 = Instant::now();
    let payload = decode_snapshot_envelope(&envelope).unwrap();
    let t_dec = t2.elapsed();
    let t3 = Instant::now();
    let again = encode_snapshot_envelope(&payload, true, 3).unwrap();
    let t_enc = t3.elapsed();
    std::fs::create_dir_all(out_dir).unwrap();
    std::fs::write(format!("{out_dir}/payload.bin"), &payload).unwrap();
    std::fs::write(format!("{out_dir}/envelope_native.zmsn"), &envelope).unwrap();
    println!(
        "{{\"event\":\"payload-gen\",\"rows\":{rows},\"payload_bytes\":{},\"payload_crc32\":{},\"native_envelope_bytes\":{},\"native_reencode_bytes\":{},\"fill_ms\":{},\"create_snapshot_ms\":{},\"native_zstd_decode_ms\":{},\"native_zstd3_encode_ms\":{}}}",
        payload.len(),
        crc32fast::hash(&payload),
        envelope.len(),
        again.len(),
        t_fill.as_millis(),
        t_snap.as_millis(),
        t_dec.as_millis(),
        t_enc.as_millis()
    );
}

fn verify(payload_path: &str, envelopes: &[String]) {
    let payload = std::fs::read(payload_path).unwrap();
    let mut all_ok = true;
    for e in envelopes {
        let bytes = std::fs::read(e).unwrap();
        let t = Instant::now();
        let res = decode_snapshot_envelope(&bytes);
        let ms = t.elapsed().as_millis();
        let (ok, detail) = match res {
            Ok(d) => (d == payload, format!("decoded {} bytes, crc32 {}", d.len(), crc32fast::hash(&d))),
            Err(err) => (false, format!("error: {err}")),
        };
        all_ok &= ok;
        println!(
            "{{\"event\":\"native-verify\",\"envelope\":\"{e}\",\"envelope_bytes\":{},\"compression_flag\":{},\"same_bytes_as_payload\":{ok},\"native_decode_ms\":{ms},\"detail\":\"{detail}\"}}",
            bytes.len(),
            bytes.get(5).copied().unwrap_or(255)
        );
    }
    if !all_ok {
        std::process::exit(1);
    }
}
