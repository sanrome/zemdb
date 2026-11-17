//! The reference app (`examples/ranking`): each of its scenarios, against a server of its own on
//! a local socket, with the devices reaching it over HTTP.
#![cfg(feature = "native")]

#[path = "../examples/ranking/app.rs"]
mod app;
// The example runs every scenario of its list in order; here each is a test.
#[allow(dead_code)]
#[path = "../examples/ranking/scenarios.rs"]
mod scenarios;

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
