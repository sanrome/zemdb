// The suites of the harness (README.md). Each suite is a list of actions, which the page runs
// in order:
//   { run: 'step' }      runs a step, an export of the steps crate, in the Worker of the page;
//   { reload: true }     reloads the page, which goes on with the next action;
//   { peer: 'step' }     runs a step in the Worker of a second page of the same context, which
//                        the page opens the first time;
//   { terminate: true }  terminates the Worker of the page (the next `run` starts another one);
//   { closePeer: true }  closes the second page.
// A suite does not reload while its second page is open: the page would lose it.
export const suites = {
  reload: [{ run: 'reload_write' }, { reload: true }, { run: 'reload_read' }],
  tabs: [
    { run: 'tabs_hold' },
    { peer: 'tabs_taken' },
    { terminate: true },
    { peer: 'tabs_free' },
    { closePeer: true },
  ],
  server: [{ run: 'server_register' }, { run: 'server_offline' }],
  // The directory of the file layer of the storage over the OPFS, on the real layer.
  files: [
    { run: 'files_write' },
    { reload: true },
    { run: 'files_after_reload' },
    { run: 'files_taken' },
    { run: 'files_refused' },
    { run: 'files_dropped' },
    { run: 'files_errors' },
  ],
  // The files of the file layer of the storage over the OPFS, on the real layer: the openings,
  // the I/O, the rename of an open file, a file taken (also by the Worker of the second page) and
  // operations dropped by their callers.
  handles: [
    { run: 'handles_write' },
    { run: 'handles_rename' },
    { reload: true },
    { run: 'handles_after_reload' },
    { run: 'handles_generations' },
    { run: 'handles_hold' },
    { peer: 'handles_taken' },
    { run: 'handles_release' },
    { peer: 'handles_free' },
    { closePeer: true },
    { run: 'handles_dropped' },
  ],
  // The disk engine of the storage on the file layer over the OPFS, the real engine on the real
  // layer: a room open across a reload, with its automatic compactions; the Worker terminated with
  // a loop of batches, of compactions, of snapshot installations and of deletions in flight, and
  // the room that reopens at a prefix of its log; snapshots of the native engine and of its own,
  // across a reload; a second page that opens a room that the first one has open; and operations
  // dropped by their callers.
  engine: [
    { run: 'engine_write' },
    { reload: true },
    { run: 'engine_after_reload' },
    { run: 'engine_batches_in_flight' },
    { terminate: true },
    { run: 'engine_batches_cut' },
    { run: 'engine_compactions_in_flight' },
    { terminate: true },
    { run: 'engine_compactions_cut' },
    { run: 'engine_snapshots_in_flight' },
    { terminate: true },
    { run: 'engine_snapshots_cut' },
    { run: 'engine_deletes_in_flight' },
    { terminate: true },
    { run: 'engine_deletes_cut' },
    { run: 'engine_snapshots' },
    { reload: true },
    { run: 'engine_snapshots_after_reload' },
    { run: 'engine_hold' },
    { peer: 'engine_locked' },
    { run: 'engine_release' },
    { peer: 'engine_free' },
    { closePeer: true },
    { run: 'engine_dropped' },
  ],
  // Only the runner runs these: `isolation` in another context of the browser, with the run id
  // of a run of `reload`; `rooms`, the time of opening tens of rooms, in another context too, so
  // that its steps have the time of a page of their own; `quota` in Chromium, in another context
  // whose quota it makes small; the other three in its self-check, which checks that a step that
  // fails, in either page, or that panics, fails the run with its name and its error.
  isolation: [{ run: 'reload_isolated' }],
  rooms: [{ run: 'engine_rooms_10' }, { run: 'engine_rooms_30' }, { run: 'engine_rooms_60' }],
  quota: [{ run: 'handles_quota' }],
  fails: [{ run: 'fails_on_purpose' }],
  fails_in_peer: [{ peer: 'fails_on_purpose' }],
  panics: [{ run: 'panics_on_purpose' }],
};

// The suites of a page whose URL names none.
export const defaults = ['reload', 'tabs', 'server', 'files', 'handles', 'engine'];
