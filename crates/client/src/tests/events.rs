use super::*;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD};
use base64::Engine;
use zemdb_core::{
    encode_message, CorrelationId, DataType, ErrorCode, Operation, RowBuilder, Schema,
    ServerMessage, TableSchema,
};

/// Every event the parser gives for `blocks`, read one block at a time, and the error that
/// stopped it, if any.
fn parse(blocks: &[&[u8]]) -> (Vec<SseEvent>, Option<StreamError>) {
    let mut parser = EventParser::default();
    let mut events = Vec::new();
    for block in blocks {
        parser.push(Bytes::copy_from_slice(block));
        loop {
            match parser.next_event() {
                Ok(Some(event)) => events.push(event),
                Ok(None) => break,
                Err(err) => return (events, Some(err)),
            }
        }
    }
    (events, None)
}

/// The events the parser gives for `bytes` in one block, failing on an error.
fn events(bytes: &[u8]) -> Vec<SseEvent> {
    let (events, failed) = parse(&[bytes]);
    assert_eq!(failed, None);
    events
}

fn event(name: &str, data: &str) -> SseEvent {
    SseEvent {
        name: name.to_string(),
        data: data.to_string(),
    }
}

fn room() -> RoomId {
    RoomId::new("room-a").unwrap()
}

fn insert(id: i64) -> Operation {
    let table = TableSchema::builder("tasks")
        .primary_key("id", DataType::Int)
        .column("title", DataType::String);
    let row = RowBuilder::new().set("id", id).set("title", "task").build();
    Schema::builder()
        .table(table)
        .build()
        .to_operation_insert("tasks", &row, 1000)
        .unwrap()
}

/// The frame of an `ops` event of `room_id` with the operations of sequences 1 to `len`.
fn ops_frame(len: u64) -> Vec<u8> {
    encode_message(&ServerMessage::OpsEvent {
        room_id: room(),
        room_incarnation: 7,
        head_seq: SequenceNumber::new(len),
        ops: (1..=len)
            .map(|seq| SequencedOperation::new(seq, insert(seq as i64)))
            .collect(),
        schema_version: 1,
        schema_id: SchemaId::new("tasks").unwrap(),
    })
    .unwrap()
}

/// The events of the server, as it writes them: an `ops` and a keep-alive between them.
fn server_stream() -> String {
    format!(
        "event: ops\ndata: {}\n\n:\n\nevent: head_advanced\ndata: 7\n\n\
         event: schema_reloaded\ndata: tasks\n\nevent: snapshot_wanted\ndata: \n\n\
         event: snapshot_available\ndata: 9\n\nevent: lagged\ndata: true\n\n",
        STANDARD.encode(ops_frame(3))
    )
}

#[test]
fn an_event_split_anywhere_between_blocks_is_read_whole() {
    let stream = server_stream();
    let bytes = stream.as_bytes();
    let whole = events(bytes);
    assert_eq!(whole.len(), 6);
    for at in 0..=bytes.len() {
        let (split, failed) = parse(&[&bytes[..at], &bytes[at..]]);
        assert_eq!((split, failed), (whole.clone(), None), "Split at {at}");
    }
    let bytes: Vec<&[u8]> = bytes.chunks(1).collect();
    assert_eq!(parse(&bytes), (whole, None));
}

#[test]
fn the_data_lines_of_an_event_are_joined_with_line_feeds() {
    assert_eq!(
        events(b"data: a\ndata:b\ndata:  c\ndata\ndata:\n\n"),
        [event("message", "a\nb\n c\n\n")]
    );
}

#[test]
fn lines_end_with_a_line_feed_a_carriage_return_or_both() {
    let crlf = events(b"event: lagged\r\ndata: true\r\n\r\nevent: x\rdata: 1\r\rdata: 2\n\n");
    assert_eq!(
        crlf,
        [
            event("lagged", "true"),
            event("x", "1"),
            event("message", "2")
        ]
    );
    // A carriage return that ends a block, and the line feed that starts the next, end one line.
    let split = parse(&[b"data: a\r", b"\ndata: b\r", b"\n\r", b"\n"]);
    assert_eq!(split, (vec![event("message", "a\nb")], None));
}

#[test]
fn comments_and_unknown_fields_are_skipped() {
    let skipped =
        events(b":\n\n: keep-alive\nid: 5\nretry: 10\nfoo: bar\nevent: lagged\ndata: true\n\n");
    assert_eq!(skipped, [event("lagged", "true")]);
}

#[test]
fn an_event_without_data_is_not_given_and_its_name_does_not_last() {
    assert_eq!(
        events(b"event: lagged\n\ndata: x\n\nevent: a\nevent: b\ndata: y\n\n"),
        [event("message", "x"), event("b", "y")]
    );
    // What comes after the last blank line is not an event yet.
    assert_eq!(events(b"event: lagged\ndata: true\n"), []);
}

#[test]
fn the_events_of_the_server_decode() {
    let decoded: Vec<RoomEvent> = events(server_stream().as_bytes())
        .iter()
        .map(|event| event.decode().unwrap())
        .collect();
    let RoomEvent::Ops(ops) = &decoded[0] else {
        panic!("{:?}", decoded[0]);
    };
    assert_eq!(
        (
            &ops.room_id,
            ops.room_incarnation,
            ops.head_seq,
            ops.ops.len()
        ),
        (&room(), 7, SequenceNumber::new(3), 3)
    );
    assert_eq!(ops.ops[2], SequencedOperation::new(3, insert(3)));
    assert_eq!(
        (ops.schema_version, &ops.schema_id),
        (1, &SchemaId::new("tasks").unwrap())
    );
    assert_eq!(
        decoded[1..],
        [
            RoomEvent::HeadAdvanced(SequenceNumber::new(7)),
            RoomEvent::SchemaReloaded,
            RoomEvent::SnapshotWanted,
            RoomEvent::SnapshotAvailable(SequenceNumber::new(9)),
            RoomEvent::Lagged,
        ]
    );
    for name in ["message", "foo", "OPS"] {
        assert_eq!(event(name, "x").decode().unwrap(), RoomEvent::Unknown);
    }
}

#[test]
fn an_event_up_to_the_limit_is_read_and_one_longer_closes_the_stream() {
    // A line of exactly the limit, in pieces.
    let line = format!("data:{}", "a".repeat(MAX_EVENT_BYTES - 5));
    let (read, failed) = parse(&[&line.as_bytes()[..100], &line.as_bytes()[100..], b"\n\n"]);
    assert_eq!((read.len(), failed), (1, None));

    // A line one byte longer, before its end arrives.
    let long = format!("{line}a");
    let (read, failed) = parse(&[&long.as_bytes()[..100], &long.as_bytes()[100..]]);
    assert_eq!((read, failed), (vec![], Some(StreamError::TooLong)));

    // An event of short lines, comments included, that passes the limit.
    let lines = ": comment\n".repeat(MAX_EVENT_BYTES / 9 + 1);
    assert_eq!(parse(&[lines.as_bytes()]).1, Some(StreamError::TooLong));
    let lines = format!("{}\n", ": comment\n".repeat(MAX_EVENT_BYTES / 9));
    assert_eq!(parse(&[lines.as_bytes()]), (vec![], None));
}

#[test]
fn the_limit_is_of_each_event_not_of_the_stream() {
    let one = format!("data: {}\n\n", "a".repeat(MAX_EVENT_BYTES / 4));
    let stream = one.repeat(8);
    let (read, failed) = parse(&[stream.as_bytes()]);
    assert_eq!((read.len(), failed), (8, None));
}

#[test]
fn asking_again_without_a_new_block_gives_nothing_more() {
    let mut parser = EventParser::default();
    parser.push(Bytes::from_static(b"data: ab"));
    assert_eq!(parser.next_event(), Ok(None));
    assert_eq!(parser.next_event(), Ok(None));
    parser.push(Bytes::from_static(b"\n\n"));
    assert_eq!(parser.next_event(), Ok(Some(event("message", "ab"))));
}

#[test]
fn a_carriage_return_ends_its_line_and_not_a_later_one() {
    // The line feed that starts the second block ends the second line, not the first.
    let read = parse(&[b"data: a\rdata: b", b"\n\n"]);
    assert_eq!(read, (vec![event("message", "a\nb")], None));
}

#[test]
fn events_without_data_do_not_add_up_to_the_limit() {
    // 70,000 keep-alives, about 12 days of a stream that only stays alive, and then an event.
    let stream = format!("{}data: x\n\n", ":\n\n".repeat(70_000));
    assert_eq!(events(stream.as_bytes()), [event("message", "x")]);
    // An event without data, and one of almost the limit.
    let stream = format!(
        "event: a\n: {}\n\ndata: {}\n\n",
        "c".repeat(MAX_EVENT_BYTES / 2),
        "d".repeat(MAX_EVENT_BYTES - 10)
    );
    assert_eq!(parse(&[stream.as_bytes()]).1, None);
}

#[test]
fn a_field_without_a_colon_has_an_empty_value() {
    assert_eq!(events(b"data\ndata\n\n"), [event("message", "\n")]);
    assert_eq!(events(b"event\ndata: x\n\n"), [event("message", "x")]);
}

#[test]
fn the_buffers_of_an_event_of_the_limit_take_at_most_twice_the_limit() {
    // A data line of the limit that arrives in two pieces, the second of one byte: the line
    // grows to twice its first piece, and the data to twice the line once it ends.
    let mut parser = EventParser::default();
    let held = |parser: &EventParser| {
        parser.line.capacity() + parser.data.capacity() + parser.name.capacity()
    };
    let line = format!("data: {}", "a".repeat(MAX_EVENT_BYTES - 6));
    let (first, last) = line.as_bytes().split_at(line.len() - 1);
    let mut most = 0;
    for block in [first, last, b"\n"] {
        parser.push(Bytes::copy_from_slice(block));
        assert_eq!(parser.next_event(), Ok(None));
        most = most.max(held(&parser));
    }
    assert!(most <= 2 * MAX_EVENT_BYTES, "{most}");
    parser.push(Bytes::from_static(b"\n"));
    assert!(matches!(parser.next_event(), Ok(Some(_))));
    assert_eq!(held(&parser), 0);
}

#[test]
fn after_an_error_the_parser_keeps_nothing_and_reads_no_further() {
    let mut parser = EventParser::default();
    parser.push(Bytes::from(vec![b'a'; MAX_EVENT_BYTES + 1]));
    assert_eq!(parser.next_event(), Err(StreamError::TooLong));
    let kept = |parser: &EventParser| {
        let (name, data) = (parser.name.capacity(), parser.data.capacity());
        (parser.pending.len(), parser.line.capacity(), name, data)
    };
    assert_eq!(kept(&parser), (0, 0, 0, 0));
    parser.push(Bytes::from_static(b"\n\ndata: x\n\n"));
    assert_eq!(parser.next_event(), Err(StreamError::TooLong));
    assert_eq!(kept(&parser), (0, 0, 0, 0));
}

#[test]
fn bytes_that_are_not_utf8_close_the_stream() {
    for bytes in [
        &b"data: \xff\n\n"[..],
        b": \xc3\n\n",
        b"event: \xed\xa0\x80\ndata: x\n\n",
    ] {
        assert_eq!(parse(&[bytes]).1, Some(StreamError::NotUtf8), "{bytes:?}");
    }
    // A character split between two blocks is not.
    let text = "data: año\n\n".as_bytes();
    assert_eq!(
        parse(&[&text[..8], &text[8..]]).0,
        [event("message", "año")]
    );
}

#[test]
fn an_event_that_does_not_decode_is_a_typed_error() {
    let frame = ops_frame(2);
    let error = encode_message(&ServerMessage::Error {
        correlation_id: Some(CorrelationId::new(1)),
        room_id: Some(room()),
        code: ErrorCode::Unauthorized,
        message: "x".to_string(),
    })
    .unwrap();
    let decoded = |name: &str, data: String| event(name, &data).decode();
    for data in [
        "@@@@".to_string(),
        "YWJjZA=".to_string(),
        "YWJj ZA==".to_string(),
        STANDARD_NO_PAD.encode(b"abcd"),
    ] {
        assert!(matches!(decoded("ops", data), Err(EventError::Base64)));
    }
    for data in [
        String::new(),
        STANDARD.encode(b"not a frame"),
        STANDARD.encode(&frame[..frame.len() - 1]),
        STANDARD.encode([&frame[..], b"x"].concat()),
    ] {
        assert!(matches!(decoded("ops", data), Err(EventError::Frame(_))));
    }
    let schema = encode_message(&ServerMessage::Schema {
        correlation_id: CorrelationId::new(1),
        room_id: room(),
        schema_id: SchemaId::new("tasks").unwrap(),
        schema: Schema::builder().build(),
    })
    .unwrap();
    for frame in [error, schema] {
        let decoded = decoded("ops", STANDARD.encode(frame));
        assert!(matches!(decoded, Err(EventError::NotOps)), "{decoded:?}");
    }
    for name in ["head_advanced", "snapshot_available"] {
        for data in ["", "x", "-1", "1.5", "18446744073709551616"] {
            let decoded = decoded(name, data.to_string());
            assert!(
                matches!(decoded, Err(EventError::Sequence(_))),
                "{decoded:?}"
            );
        }
    }
}

/// A reader of a stream of `blocks`.
fn reader_of(blocks: Vec<Result<Bytes, TransportError>>) -> EventReader {
    EventReader::new(Box::pin(futures::stream::iter(blocks)))
}

#[tokio::test]
async fn the_reader_gives_the_events_of_the_blocks_until_the_stream_ends() {
    let blocks = vec![
        Ok(Bytes::from_static(b"event: lagged\nda")),
        Ok(Bytes::from_static(b"ta: true\n\n:\n\ndata: x\n\n")),
    ];
    let mut reader = reader_of(blocks);
    assert_eq!(reader.next().await, Ok(Some(event("lagged", "true"))));
    assert_eq!(reader.next().await, Ok(Some(event("message", "x"))));
    assert_eq!(reader.next().await, Ok(None));
    assert_eq!(reader.next().await, Ok(None));
}

#[tokio::test]
async fn an_error_of_the_transport_or_of_the_parser_ends_the_reader() {
    let cut = TransportError("Cut".to_string());
    let mut reader = reader_of(vec![
        Ok(Bytes::from_static(b"data: x\n\n")),
        Err(cut.clone()),
        Ok(Bytes::from_static(b"data: y\n\n")),
    ]);
    assert_eq!(reader.next().await, Ok(Some(event("message", "x"))));
    assert_eq!(
        reader.next().await,
        Err(StreamError::Transport(cut.clone()))
    );
    assert_eq!(reader.next().await, Err(StreamError::Transport(cut)));

    let mut reader = reader_of(vec![
        Ok(Bytes::from_static(b"data: \xff\n\n")),
        Ok(Bytes::from_static(b"data: y\n\n")),
    ]);
    assert_eq!(reader.next().await, Err(StreamError::NotUtf8));
    assert_eq!(reader.next().await, Err(StreamError::NotUtf8));
}

#[tokio::test]
async fn the_reader_does_not_ask_again_a_stream_that_ended() {
    // An unfold panics if it is asked again after it ended, as the stream of the HTTP
    // transport would.
    let block = Some(Bytes::from_static(b"data: x\n\n"));
    let blocks = futures::stream::unfold(block, |block| async move {
        block.map(|block| (Ok::<_, TransportError>(block), None))
    });
    let mut reader = EventReader::new(Box::pin(blocks));
    assert_eq!(reader.next().await, Ok(Some(event("message", "x"))));
    assert_eq!(reader.next().await, Ok(None));
    assert_eq!(reader.next().await, Ok(None));
}

#[tokio::test(start_paused = true)]
async fn a_reader_cut_while_it_waits_for_a_block_loses_nothing() {
    // Pieces of events 10 ms apart, read with a timeout of 3 ms: most reads are dropped while
    // they wait for a block.
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let blocks = futures::stream::unfold(receiver, |mut receiver| async move {
        let block: Option<Result<Bytes, TransportError>> = receiver.recv().await;
        block.map(|block| (block, receiver))
    });
    let mut reader = EventReader::new(Box::pin(blocks));
    let stream = "event: a\ndata: 1\n\nevent: b\r\ndata: 2\r\n\r\n:\n\ndata: 3\rdata: 4\r\r";
    tokio::spawn(async move {
        for piece in stream.as_bytes().chunks(3) {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            sender.send(Ok(Bytes::copy_from_slice(piece))).unwrap();
        }
    });
    let (mut read, mut cut) = (Vec::new(), 0);
    while read.len() < 3 {
        let next = reader.next();
        match tokio::time::timeout(std::time::Duration::from_millis(3), next).await {
            Ok(event) => read.push(event.unwrap().unwrap()),
            Err(_) => cut += 1,
        }
    }
    assert!(cut > 10, "{cut}");
    assert_eq!(
        read,
        [event("a", "1"), event("b", "2"), event("message", "3\n4")]
    );
}

/// A pseudo-random generator for the fuzzing (xorshift64*).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// Bytes at random, mostly pieces of the syntax of a stream.
fn random_stream(rng: &mut Rng) -> Vec<u8> {
    const PIECES: [&[u8]; 12] = [
        b"data:",
        b"data: ",
        b"event: ",
        b"ops",
        b"\n",
        b"\r",
        b"\r\n",
        b":",
        b" ",
        b"head_advanced",
        b"\xc3\xb1",
        b"\n\n",
    ];
    let mut bytes = Vec::new();
    for _ in 0..rng.below(400) {
        match rng.below(100) {
            0..10 => bytes.push(rng.next() as u8),
            10 => bytes.extend(std::iter::repeat_n(b'a', rng.below(2 * MAX_EVENT_BYTES))),
            11..20 => bytes.extend(STANDARD.encode(ops_frame(1 + rng.below(4) as u64)).bytes()),
            _ => bytes.extend_from_slice(PIECES[rng.below(PIECES.len())]),
        }
    }
    bytes
}

/// The events of the server, damaged: a byte changed, removed or added, or cut.
fn damaged_stream(rng: &mut Rng) -> Vec<u8> {
    let mut bytes = server_stream().into_bytes();
    for _ in 0..1 + rng.below(4) {
        let at = rng.below(bytes.len());
        match rng.below(4) {
            0 => bytes[at] = rng.next() as u8,
            1 => drop(bytes.remove(at)),
            2 => bytes.insert(at, rng.next() as u8),
            _ => bytes.truncate(at),
        }
        if bytes.is_empty() {
            break;
        }
    }
    bytes
}

/// `bytes` in blocks of lengths at random.
fn random_blocks<'a>(rng: &mut Rng, mut bytes: &'a [u8]) -> Vec<&'a [u8]> {
    let mut blocks = Vec::new();
    while !bytes.is_empty() {
        let longest = 1 << rng.below(18);
        let len = 1 + rng.below(bytes.len().min(longest));
        let (block, rest) = bytes.split_at(len);
        blocks.push(block);
        bytes = rest;
    }
    blocks
}

/// The events of `bytes` and the error that stops them, as the format of the stream defines
/// them, over the whole text at once: a model of the parser that shares no code with it.
fn model(bytes: &[u8]) -> (Vec<SseEvent>, Option<StreamError>) {
    let (mut events, mut name, mut data, mut size) = (vec![], String::new(), String::new(), 0);
    let mut rest = bytes;
    loop {
        let end = rest.iter().position(|&b| b == b'\n' || b == b'\r');
        size += end.unwrap_or(rest.len());
        if size > MAX_EVENT_BYTES {
            return (events, Some(StreamError::TooLong));
        }
        let Some(end) = end else {
            return (events, None);
        };
        let Ok(line) = std::str::from_utf8(&rest[..end]) else {
            return (events, Some(StreamError::NotUtf8));
        };
        let crlf = rest[end] == b'\r' && rest.get(end + 1) == Some(&b'\n');
        rest = &rest[end + 1 + usize::from(crlf)..];
        if line.is_empty() {
            if !data.is_empty() {
                data.pop();
                let name = if name.is_empty() { "message" } else { &name };
                events.push(event(name, &data));
            }
            (name, data, size) = (String::new(), String::new(), 0);
            continue;
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "" => {}
            "event" => name = value.to_string(),
            "data" => data = format!("{data}{value}\n"),
            _ => {}
        }
    }
}

/// Parses streams at random, and the events of the server damaged, in blocks at random: no
/// panic, the events and the error of the model, no event over the limit, and each one decodes
/// or is a typed error.
fn fuzz(seed: u64, runs: usize) {
    let mut rng = Rng(seed | 1);
    for run in 0..runs {
        let bytes = if run % 2 == 0 {
            random_stream(&mut rng)
        } else {
            damaged_stream(&mut rng)
        };
        let expected = model(&bytes);
        let blocks = random_blocks(&mut rng, &bytes);
        assert_eq!(parse(&blocks), expected, "Seed {seed}, run {run}");
        for event in &expected.0 {
            assert!(event.name.len() + event.data.len() <= MAX_EVENT_BYTES);
            let _ = event.decode();
        }
    }
}

#[test]
fn random_and_damaged_streams_are_read_without_panics_however_they_are_split() {
    fuzz(1, 300);
}

#[test]
#[ignore = "long: run with --ignored"]
fn random_and_damaged_streams_are_read_without_panics_however_they_are_split_long() {
    for seed in 1..=20 {
        fuzz(seed, 2_000);
    }
}
