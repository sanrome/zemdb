//! Test infrastructure: fault injection points for crash and I/O failure tests.
//!
//! Production code marks the spots where a test must inject a failure or hold an operation in
//! the middle, or check which thread runs some work. Outside test builds every point does
//! nothing (or returns its input) and the compiler removes it. A test arms a point for a specific file path, so tests running in
//! parallel never interfere with each other, and each armed point fires once. A crash plan
//! instead counts every point reached for the files of a directory, so that a simulation can
//! crash or fail its scenario at each of them.

use crate::error::StorageError;
use std::path::Path;

#[cfg(test)]
static ARMED: std::sync::Mutex<Vec<(&'static str, std::path::PathBuf)>> =
    std::sync::Mutex::new(Vec::new());

/// A pause point armed for one path: signals `reached` when hit, then waits for `release`.
#[cfg(test)]
type ArmedPause = (
    &'static str,
    std::path::PathBuf,
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Receiver<()>,
);

#[cfg(test)]
static PAUSES: std::sync::Mutex<Vec<ArmedPause>> = std::sync::Mutex::new(Vec::new());

/// Removes and returns the first entry of `list` that `hit` matches.
#[cfg(test)]
fn take<T>(list: &std::sync::Mutex<Vec<T>>, hit: impl Fn(&T) -> bool) -> Option<T> {
    let mut list = list.lock().unwrap();
    list.iter().position(hit).map(|pos| list.remove(pos))
}

/// Arms the fail point `name` for `path`; the next `check` with the same pair fails.
#[cfg(test)]
pub(crate) fn arm(name: &'static str, path: &Path) {
    ARMED.lock().unwrap().push((name, path.to_path_buf()));
}

/// Disarms the fail point `name` for `path` if no `check` has consumed it, so that an operation
/// that never reached it does not leave it armed for a later one.
#[cfg(test)]
pub(crate) fn disarm(name: &'static str, path: &Path) {
    take(&ARMED, |(n, p)| *n == name && p == path);
}

/// What a crash plan does at the points it counts.
///
/// Copying the files of an open room reads files its engine holds locked, which Windows refuses,
/// so only Unix test builds copy them.
#[cfg(test)]
pub(crate) enum CrashAction {
    /// Copies the files of the plan's directory, at the `n`-th point, to the directory `n`
    /// (counting from 1) inside this one: what the disk would hold if the process crashed there.
    #[cfg_attr(not(unix), allow(dead_code))]
    CopyAtEveryPoint(std::path::PathBuf),
    /// Copies the files of the plan's directory to `to` at the `at`-th point.
    #[cfg_attr(not(unix), allow(dead_code))]
    CopyAt { at: usize, to: std::path::PathBuf },
    /// Makes the `at`-th point fail as if it were armed: an I/O error, or a panic at a panic
    /// point. A pause point cannot fail, so there it has no effect.
    FailAt(usize),
}

/// A crash plan for the files under `root`: counts every fail and pause point reached for a path
/// there, in order, and takes its action at them.
#[cfg(test)]
struct CrashPlan {
    root: std::path::PathBuf,
    action: CrashAction,
    points: Vec<&'static str>,
}

#[cfg(test)]
static CRASH_PLANS: std::sync::Mutex<Vec<CrashPlan>> = std::sync::Mutex::new(Vec::new());

/// The crash plans. A panic injected by a plan is raised after the lock is released, and a test
/// that fails while holding it leaves the plans consistent, so a poisoned lock is used as is.
#[cfg(test)]
fn crash_plans() -> std::sync::MutexGuard<'static, Vec<CrashPlan>> {
    CRASH_PLANS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Arms a crash plan with `action` for the files under `root`, replacing any plan already armed
/// for it.
#[cfg(test)]
pub(crate) fn arm_crash_plan(root: &Path, action: CrashAction) {
    let mut plans = crash_plans();
    plans.retain(|plan| plan.root != root);
    plans.push(CrashPlan {
        root: root.to_path_buf(),
        action,
        points: Vec::new(),
    });
}

/// How many points the crash plan of `root` has counted so far.
#[cfg(test)]
pub(crate) fn crash_plan_points(root: &Path) -> usize {
    crash_plans()
        .iter()
        .find(|plan| plan.root == root)
        .map_or(0, |plan| plan.points.len())
}

/// Disarms the crash plan of `root` and returns the points it counted, in order.
#[cfg(test)]
pub(crate) fn disarm_crash_plan(root: &Path) -> Vec<&'static str> {
    let mut plans = crash_plans();
    let pos = plans
        .iter()
        .position(|plan| plan.root == root)
        .expect("no crash plan armed for this directory");
    plans.remove(pos).points
}

/// Copies the regular files of `from` to `to`, which is created if needed. A file removed while
/// the copy runs is left out, as it would be from the disk.
#[cfg(test)]
pub(crate) fn copy_files(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().is_ok_and(|kind| kind.is_file()) {
            let _ = std::fs::copy(entry.path(), to.join(entry.file_name()));
        }
    }
}

/// Counts the point `name`, reached for `path`, in the crash plan armed for a directory holding
/// `path`, if any, and takes the plan's action there. Returns whether the point must fail.
#[cfg(test)]
fn crash_plan_point(name: &'static str, path: &Path) -> bool {
    let mut plans = crash_plans();
    let Some(plan) = plans.iter_mut().find(|plan| path.starts_with(&plan.root)) else {
        return false;
    };
    plan.points.push(name);
    let point = plan.points.len();
    match &plan.action {
        CrashAction::CopyAtEveryPoint(images) => {
            copy_files(&plan.root, &images.join(point.to_string()));
            false
        }
        CrashAction::CopyAt { at, to } => {
            if point == *at {
                copy_files(&plan.root, to);
            }
            false
        }
        CrashAction::FailAt(at) => point == *at,
    }
}

/// Returns an injected I/O error if the fail point `name` is armed for `path`.
#[inline(always)]
pub(crate) fn check(name: &'static str, path: &Path) -> Result<(), StorageError> {
    #[cfg(test)]
    if crash_plan_point(name, path) || take(&ARMED, |(n, p)| *n == name && p == path).is_some() {
        return Err(StorageError::Io(std::io::Error::other(format!(
            "injected failure at {name}"
        ))));
    }
    #[cfg(not(test))]
    let _ = (name, path);
    Ok(())
}

/// Panics if the fail point `name` is armed for `path`, to reproduce a panic at that spot.
#[inline(always)]
pub(crate) fn panic_point(name: &'static str, path: &Path) {
    if check(name, path).is_err() {
        panic!("injected panic at {name}");
    }
}

/// Returns an injected I/O error instead of `result` if the fail point `name` is armed for
/// `path`, as if the call that produced `result` had failed.
#[inline(always)]
pub(crate) fn io_result<T>(
    name: &'static str,
    path: &Path,
    result: std::io::Result<T>,
) -> std::io::Result<T> {
    if check(name, path).is_err() {
        return Err(std::io::Error::other(format!("injected failure at {name}")));
    }
    result
}

/// Replaces `file`, open on `file_path`, with a read-only handle of the same file if the fail
/// point `name` is armed for `path`. The writes that follow are accepted by `write_all` and then
/// fail on the blocking pool, as on a full disk. Writes already issued through `file` complete
/// first.
///
/// Dropping the replaced handle releases any lock it held, such as the room's exclusive lock on
/// its WAL; only tests that arm this point are affected.
#[inline(always)]
pub(crate) async fn read_only_handle(
    name: &'static str,
    path: &Path,
    file: &mut tokio::fs::File,
    file_path: &Path,
) {
    if check(name, path).is_err() {
        use tokio::io::AsyncWriteExt;
        file.flush().await.unwrap();
        *file = tokio::fs::File::from_std(std::fs::File::open(file_path).unwrap());
    }
}

/// Handle of an armed pause point, held by the test.
#[cfg(test)]
pub(crate) struct Pause {
    reached: tokio::sync::oneshot::Receiver<()>,
    release: tokio::sync::oneshot::Sender<()>,
}

#[cfg(test)]
impl Pause {
    /// Waits until the operation reaches the pause point.
    pub(crate) async fn reached(&mut self) {
        (&mut self.reached).await.unwrap();
    }

    /// Lets the paused operation continue.
    pub(crate) fn release(self) {
        let _ = self.release.send(());
    }
}

/// Arms the pause point `name` for `path`: the next `pause` with the same pair waits until the
/// returned handle is released.
#[cfg(test)]
pub(crate) fn arm_pause(name: &'static str, path: &Path) -> Pause {
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    PAUSES
        .lock()
        .unwrap()
        .push((name, path.to_path_buf(), reached_tx, release_rx));
    Pause {
        reached: reached_rx,
        release: release_tx,
    }
}

/// Waits for the test to release the pause point `name` if it is armed for `path`.
#[inline(always)]
pub(crate) async fn pause(name: &'static str, path: &Path) {
    #[cfg(test)]
    crash_plan_point(name, path);
    #[cfg(test)]
    if let Some((_, _, reached, release)) = take(&PAUSES, |(n, p, _, _)| *n == name && p == path) {
        let _ = reached.send(());
        let _ = release.await;
    }
    #[cfg(not(test))]
    let _ = (name, path);
}

/// How long a blocking pause waits to be released before it panics.
#[cfg(test)]
const BLOCKING_PAUSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// A blocking pause point armed for one path: signals `reached` when hit, then blocks its thread
/// until `release`.
#[cfg(test)]
type ArmedBlockingPause = (
    &'static str,
    std::path::PathBuf,
    tokio::sync::oneshot::Sender<()>,
    std::sync::mpsc::Receiver<()>,
);

#[cfg(test)]
static BLOCKING_PAUSES: std::sync::Mutex<Vec<ArmedBlockingPause>> =
    std::sync::Mutex::new(Vec::new());

/// Handle of an armed blocking pause point, held by the test.
#[cfg(test)]
pub(crate) struct BlockingPause {
    reached: tokio::sync::oneshot::Receiver<()>,
    release: std::sync::mpsc::Sender<()>,
}

#[cfg(test)]
impl BlockingPause {
    /// Waits until the operation reaches the pause point.
    pub(crate) async fn reached(&mut self) {
        (&mut self.reached).await.unwrap();
    }

    /// Lets the paused operation continue.
    pub(crate) fn release(self) {
        let _ = self.release.send(());
    }
}

/// Arms the blocking pause point `name` for `path`: the next `pause_blocking` with the same pair
/// blocks its thread until the returned handle is released.
#[cfg(test)]
pub(crate) fn arm_blocking_pause(name: &'static str, path: &Path) -> BlockingPause {
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    BLOCKING_PAUSES
        .lock()
        .unwrap()
        .push((name, path.to_path_buf(), reached_tx, release_rx));
    BlockingPause {
        reached: reached_rx,
        release: release_tx,
    }
}

/// Blocks the calling thread until the test releases the pause point `name`, if it is armed for
/// `path`.
///
/// The test releases it from an async task, which can only run while some runtime thread is
/// free; so it panics if it is not released within a few seconds instead of hanging, which is
/// what happens when it is reached on the only thread of a current-thread runtime.
#[inline(always)]
pub(crate) fn pause_blocking(name: &'static str, path: &Path) {
    #[cfg(test)]
    crash_plan_point(name, path);
    hold_blocking(name, path);
}

/// Blocks like [`pause_blocking`], but crash plans do not count it: it holds the file operations
/// of the native file layer, which the points of the engine around them already place.
#[inline(always)]
pub(crate) fn hold_blocking(name: &'static str, path: &Path) {
    #[cfg(test)]
    if let Some((_, _, reached, release)) =
        take(&BLOCKING_PAUSES, |(n, p, _, _)| *n == name && p == path)
    {
        let _ = reached.send(());
        if release.recv_timeout(BLOCKING_PAUSE_TIMEOUT).is_err() {
            panic!("pause point {name} was not released: the runtime is blocked");
        }
    }
    #[cfg(not(test))]
    let _ = (name, path);
}

/// The thread points reached, each with a thread that reached it.
#[cfg(test)]
static THREAD_POINTS: std::sync::Mutex<Vec<(&'static str, std::thread::ThreadId)>> =
    std::sync::Mutex::new(Vec::new());

/// Records that the current thread reached the point `name`, so that a test can check that some
/// work never runs on the thread of its runtime.
#[inline(always)]
pub(crate) fn thread_point(name: &'static str) {
    #[cfg(test)]
    {
        let reached = (name, std::thread::current().id());
        let mut points = THREAD_POINTS.lock().unwrap();
        if !points.contains(&reached) {
            points.push(reached);
        }
    }
    #[cfg(not(test))]
    let _ = name;
}

/// Whether the current thread ever reached the thread point `name`.
#[cfg(test)]
pub(crate) fn reached_on_this_thread(name: &'static str) -> bool {
    let reached = (name, std::thread::current().id());
    THREAD_POINTS.lock().unwrap().contains(&reached)
}
