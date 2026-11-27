// The runner of the browser harness (README.md). Starts the test server on a free port, opens
// the page in a browser without a window with Playwright, and fails if a step fails, naming it.
//
//   node run.mjs [--browser chromium|firefox|webkit] [--server <binary>] [--suites <a,b>] [--headed]
//   node run.mjs --self-check [--browser ...] [--server ...]
//
// Without --suites it runs the default suites of the page, then the suite `isolation` in
// another context of the browser, with the same run id, and in Chromium the suite `quota` in a
// third context, whose quota it makes small through CDP. --self-check checks the runner itself:
// a run of each of the suites `fails`, `fails_in_peer` and `panics`, whose step fails or panics
// on purpose, has to fail, naming the step and giving its error.
import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';
import { randomBytes } from 'node:crypto';
import * as playwright from 'playwright';

const here = fileURLToPath(new URL('.', import.meta.url));
// The longest a page may take to run its suites, across its reloads.
const PAGE_TIMEOUT_MS = 180_000;
const SERVER_START_TIMEOUT_MS = 30_000;
// The test server closes its connections within 5 s, and then shuts its rooms down.
const SERVER_STOP_TIMEOUT_MS = 15_000;
// The quota of the origin for the suite `quota`, whose step writes over it.
const QUOTA_BYTES = 4 * 1024 * 1024;

const options = parseArgs(process.argv.slice(2));
process.exitCode = options.selfCheck ? await selfCheck() : await run();

function parseArgs(args) {
  const options = {
    browser: 'chromium',
    server: `${here}target/debug/zemdb-browser-server`,
    suites: null,
    headed: false,
    selfCheck: false,
  };
  for (let i = 0; i < args.length; i += 1) {
    const value = () => {
      i += 1;
      if (i >= args.length) {
        usage(`${args[i - 1]} takes a value`);
      }
      return args[i];
    };
    switch (args[i]) {
      case '--browser': options.browser = value(); break;
      case '--server': options.server = value(); break;
      case '--suites': options.suites = value(); break;
      case '--headed': options.headed = true; break;
      case '--self-check': options.selfCheck = true; break;
      default: usage(`unknown argument ${args[i]}`);
    }
  }
  if (!['chromium', 'firefox', 'webkit'].includes(options.browser)) {
    usage(`unknown browser ${options.browser}`);
  }
  return options;
}

function usage(problem) {
  console.error(`${problem}\nusage: node run.mjs [--browser chromium|firefox|webkit] [--server <binary>] [--suites <a,b>] [--headed] [--self-check]`);
  process.exit(2);
}

async function run() {
  if (!existsSync(`${here}www/pkg/zemdb_browser_steps_bg.wasm`)) {
    console.error('www/pkg is missing: build first (sh build.sh)');
    return 1;
  }
  const server = await startServer(options.server);
  let browser = null;
  let failed = false;
  try {
    browser = await playwright[options.browser].launch({ headless: !options.headed });
    console.log(`${options.browser} ${browser.version()}, test server at ${server.url}`);
    const runId = randomBytes(8).toString('hex');
    const pages = options.suites === null
      ? [{ query: `?run=${runId}` }, { query: `?suites=isolation&run=${runId}` }]
      : [{ query: `?suites=${encodeURIComponent(options.suites)}&run=${runId}` }];
    // The quota of an origin can be set only through CDP, which only Chromium has.
    if (options.suites === null && options.browser === 'chromium') {
      pages.push({ query: `?suites=quota&run=${runId}`, quota: QUOTA_BYTES });
    } else if (options.suites === null) {
      console.log(`quota: not run in ${options.browser}, which has no CDP to set the quota of the origin`);
    }
    for (const { query, quota } of pages) {
      // A context of its own for each page: the second default one checks the isolation, and
      // the third has a small quota.
      const context = await browser.newContext();
      const result = await runPage(context, `${server.url}${query}`, quota);
      await context.close();
      // The isolation is checked with the run id of the URL: a page that took another one would
      // look for the files of another run.
      if (result.status === 'pass' && result.run !== runId) {
        result.status = 'fail';
        result.failed = { step: 'the page', error: `ran with the run id ${result.run}, not ${runId}` };
      }
      report(result);
      if (result.status !== 'pass') {
        failed = true;
        break;
      }
    }
  } finally {
    await browser?.close();
    if (!(await server.stop())) {
      failed = true;
    }
  }
  console.log(failed ? 'FAILED' : 'passed');
  return failed ? 1 : 0;
}

// Opens `url` and waits for its result, across the reloads of the page. The console of the
// page, of its Worker and of the second page is printed if the run fails. With `quota`, the
// quota of the origin in the context is that many bytes, set through CDP before the page opens.
async function runPage(context, url, quota) {
  const log = [];
  const watch = (page, name) => {
    page.on('console', (message) => log.push(`[${name}] ${message.type()}: ${message.text()}`));
    page.on('pageerror', (error) => log.push(`[${name}] uncaught: ${error.message}`));
    page.on('crash', () => log.push(`[${name}] crashed`));
  };
  let pages = 0;
  context.on('page', (page) => {
    watch(page, pages === 0 ? 'page' : 'second page');
    pages += 1;
  });
  const page = await context.newPage();
  if (quota !== undefined) {
    const cdp = await context.newCDPSession(page);
    await cdp.send('Storage.overrideQuotaForOrigin', { origin: new URL(url).origin, quotaSize: quota });
  }
  await page.goto(url);
  const deadline = Date.now() + PAGE_TIMEOUT_MS;
  for (;;) {
    // Between steps the page reloads, and an evaluation in flight fails: it is asked again.
    const result = await page.evaluate(() => window.harnessResult ?? null).catch(() => null);
    if (result !== null) {
      // A step that the page says ran in the second page needs a second page of the browser,
      // whatever the page says.
      const second = result.results.find((step) => step.page === 'second');
      if (result.status === 'pass' && second !== undefined && pages < 2) {
        result.status = 'fail';
        result.failed = { step: second.step, error: 'ran in a second page, but the context never opened one' };
      }
      if (result.status !== 'pass') {
        result.log = log;
      }
      return result;
    }
    if (page.isClosed() || Date.now() > deadline) {
      const current = await page.evaluate(() => window.harnessProgress?.current ?? null).catch(() => null);
      const problem = page.isClosed() ? 'the page closed' : `timed out after ${PAGE_TIMEOUT_MS / 1000} s`;
      return { status: 'fail', results: [], failed: { step: current ?? 'the page', error: problem }, log };
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}

function report(result) {
  for (const step of result.results) {
    const where = step.page === 'second' ? ', second page' : '';
    console.log(`${step.ok ? 'ok  ' : 'FAIL'} ${step.step} (${step.ms} ms${where})${step.ok ? '' : `: ${step.error}`}`);
  }
  if (result.status === 'pass' && result.results.length === 0) {
    result.status = 'fail';
    result.failed = { step: 'the page', error: 'no step ran' };
  }
  if (result.status !== 'pass') {
    // Unless the step itself failed, and its line above says why.
    const failedStep = result.results.some((step) => step.step === result.failed.step && !step.ok);
    if (!failedStep) {
      console.log(`FAIL ${result.failed.step}: ${result.failed.error}`);
    }
    for (const line of result.log ?? []) {
      console.log(`  ${line}`);
    }
  }
}

// Starts the test server: resolves once it prints its URL. Closing its standard input stops it;
// `stop` resolves to whether it stopped in time, and with success.
function startServer(binary) {
  return new Promise((resolve, reject) => {
    const child = spawn(binary, ['--www', `${here}www`], { stdio: ['pipe', 'pipe', 'inherit'] });
    let exited = null;
    const exit = new Promise((resolveExit) => child.on('exit', (code, signal) => {
      exited = { code, signal };
      resolveExit(exited);
    }));
    child.on('error', (error) => reject(new Error(`the test server did not start: ${error.message}`)));
    exit.then(({ code, signal }) => reject(new Error(`the test server exited (${signal ?? code}) before it listened`)));
    const timer = setTimeout(() => {
      child.kill('SIGKILL');
      reject(new Error('the test server did not listen in time'));
    }, SERVER_START_TIMEOUT_MS);
    const lines = createInterface({ input: child.stdout });
    lines.on('line', (line) => {
      const listening = /^listening on (http:\/\/\S+)$/.exec(line);
      if (listening === null) {
        console.log(`[server] ${line}`);
        return;
      }
      clearTimeout(timer);
      resolve({
        url: listening[1],
        async stop() {
          child.stdin.end();
          let stopTimer;
          const stopped = await Promise.race([
            exit,
            new Promise((resolveStop) => {
              stopTimer = setTimeout(() => resolveStop(null), SERVER_STOP_TIMEOUT_MS);
            }),
          ]);
          clearTimeout(stopTimer);
          if (stopped === null) {
            child.kill('SIGKILL');
            console.log(`FAIL the test server did not stop within ${SERVER_STOP_TIMEOUT_MS / 1000} s of its input closing`);
            return false;
          }
          if (stopped.code !== 0) {
            console.log(`FAIL the test server stopped with ${stopped.signal ?? `code ${stopped.code}`}`);
            return false;
          }
          return true;
        },
      });
    });
  });
}

// Runs each of the suites that fail on purpose in another process of this runner, which has to
// fail, naming the step and giving its error.
async function selfCheck() {
  const checks = [
    { suite: 'fails', failure: /^FAIL fails\/fails_on_purpose \(\d+ ms\): Error: this step fails on purpose$/m },
    {
      suite: 'fails_in_peer',
      failure: /^FAIL fails_in_peer\/fails_on_purpose \(\d+ ms, second page\): Error: this step fails on purpose$/m,
    },
    { suite: 'panics', failure: /^FAIL panics\/panics_on_purpose \(\d+ ms\): panicked at [^\n]+\nthis step panics on purpose$/m },
  ];
  let failed = false;
  for (const { suite, failure } of checks) {
    const args = [fileURLToPath(import.meta.url), '--browser', options.browser, '--server', options.server, '--suites', suite];
    const child = spawn(process.execPath, args, { stdio: ['ignore', 'pipe', 'pipe'] });
    let output = '';
    child.stdout.on('data', (chunk) => { output += chunk; });
    child.stderr.on('data', (chunk) => { output += chunk; });
    const code = await new Promise((resolve) => child.on('exit', resolve));
    const named = failure.test(output);
    if (code === 1 && named) {
      console.log(`self-check: ${suite} fails the run, naming its step and giving its error`);
    } else {
      failed = true;
      console.log(`self-check FAILED: the run of ${suite} ended with code ${code}${named ? '' : ', without the expected failure'}:\n${output}`);
    }
  }
  console.log(failed ? 'self-check FAILED' : 'self-check passed');
  return failed ? 1 : 0;
}
