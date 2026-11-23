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
  // Only the runner runs these: `isolation` in another context of the browser, with the run id
  // of a run of `reload`; the other three in its self-check, which checks that a step that fails,
  // in either page, or that panics, fails the run with its name and its error.
  isolation: [{ run: 'reload_isolated' }],
  fails: [{ run: 'fails_on_purpose' }],
  fails_in_peer: [{ peer: 'fails_on_purpose' }],
  panics: [{ run: 'panics_on_purpose' }],
};

// The suites of a page whose URL names none.
export const defaults = ['reload', 'tabs', 'server'];
