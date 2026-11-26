//! The reference app (`examples/ranking`): each of its scenarios, against a server of its own on
//! a local socket, with the devices reaching it over HTTP; and, beyond them, devices that write at
//! once and end with the same rows of every table.
#![cfg(feature = "native")]
#![allow(clippy::disallowed_methods)]

#[path = "../examples/ranking/app.rs"]
mod app;
// The example runs every scenario of its list in order; here each is a test.
#[allow(dead_code)]
#[path = "../examples/ranking/scenarios.rs"]
mod scenarios;

use zemdb_client::{ClientConfig, ClientError, Freshness};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_replica_reaches_the_same_standings() {
    scenarios::same_standings().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_replica_rejects_a_second_vote_a_stranger_and_a_title_repeated() {
    scenarios::rejections().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_person_votes_from_two_devices() {
    scenarios::two_devices().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_members_keep_the_creator_from_deleting_the_ranking() {
    scenarios::restrict().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_member_catches_up_from_the_snapshot_of_another() {
    scenarios::snapshot().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_network_the_standings_read_and_a_vote_is_offline() {
    scenarios::offline().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_standings_watched_change_as_the_votes_arrive() {
    scenarios::watch().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_notes_never_travel_in_the_clear() {
    scenarios::notes().await.unwrap();
}

// ---------------------------------------------------------------------------------------------
// Beyond the scenarios

/// Every row of every table of the app in the replica of `room`, with the notes decrypted.
async fn every_row(room: &zemdb_client::RoomHandle) -> Vec<Vec<zemdb_client::Row>> {
    let mut tables = Vec::new();
    for table in ["rankings", "members", "rounds", "votes"] {
        let table = room.table(table).unwrap();
        tables.push(table.scan(&zemdb_client::Scan::new()).await.unwrap());
    }
    tables
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replicas_that_write_at_once_hold_the_same_rows_of_every_table() {
    // The convergence over the whole state, not only the standings: four devices (two of carla,
    // one of them in `manual`) write at once without catching up first, among them two rounds
    // with the same title and two votes of carla in one round, which the log decides; once every
    // replica is at the head of the room, all of them hold the same rows of every table.
    let server = app::Server::start().await.unwrap();
    let backend = server.backend();
    backend.register_schema().await.unwrap();
    let room_id = backend
        .create_room("friends", app::Retention::Default)
        .await
        .unwrap();
    let manual = |config: &mut ClientConfig| config.freshness = Freshness::Manual;
    let mut devices = Vec::new();
    for (person, manual_mode) in [
        ("ana", false),
        ("bob", false),
        ("carla", false),
        ("carla", true),
    ] {
        let device = match manual_mode {
            false => app::Device::with(&server, person, |_| {}).await.unwrap(),
            true => app::Device::with(&server, person, manual).await.unwrap(),
        };
        let room = device.room(&room_id).await.unwrap();
        devices.push((device, room));
    }
    let ranking = app::create_ranking(&devices[0].1, "Bowling").await.unwrap();
    for ((_, room), name) in devices.iter().zip(["Ana", "Bob", "Carla"]) {
        room.sync().await.unwrap();
        app::join(room, ranking, name).await.unwrap();
    }
    for (_, room) in &devices {
        room.sync().await.unwrap();
    }
    let round = app::open_round(&devices[0].1, ranking, "Round 1", "Lane 4")
        .await
        .unwrap();
    for (_, room) in &devices {
        room.sync().await.unwrap();
    }

    // At once, without catching up first.
    let rooms: Vec<_> = devices.iter().map(|(_, room)| room.clone()).collect();
    let writes = rooms.iter().enumerate().map(|(i, room)| {
        let room = room.clone();
        async move {
            let candidate = ["bob", "carla", "ana", "bob"][i];
            let voted = app::vote(&room, ranking, round, candidate).await;
            let opened = match i {
                0 | 1 => app::open_round(&room, ranking, "Round 2", "Lane 7")
                    .await
                    .map(drop),
                _ => Ok(()),
            };
            (voted, opened)
        }
    });
    let written = futures::future::join_all(writes).await;
    for (voted, opened) in &written {
        for result in [voted, opened] {
            let decided = matches!(
                result,
                Ok(()) | Err(ClientError::ConstraintViolation(_) | ClientError::PrecheckFailed(_))
            );
            assert!(decided, "{written:?}");
        }
    }
    // Every replica reaches the head of the log.
    let (_, head) = server.log(&room_id).await.unwrap();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    for room in &rooms {
        loop {
            room.sync().await.unwrap();
            if room.status().await.unwrap().head_seq.get() == head {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{:?}",
                room.status().await
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }
    // The log took one round of each title and one vote of carla in the round.
    let first = every_row(&rooms[0]).await;
    let rounds = &first[2];
    let titled = |title: &str| {
        rounds
            .iter()
            .filter(|row| row.get("title").and_then(zemdb_core::Value::as_str) == Some(title))
            .count()
    };
    assert_eq!((titled("Round 1"), titled("Round 2")), (1, 1), "{rounds:?}");
    // One vote of carla in the round, from either device.
    let carlas = (first[3].iter())
        .filter(|row| row.get("voter").and_then(zemdb_core::Value::as_str) == Some("carla"))
        .count();
    assert_eq!(carlas, 1, "{:?}", first[3]);
    for room in &rooms[1..] {
        assert_eq!(every_row(room).await, first);
    }
}
