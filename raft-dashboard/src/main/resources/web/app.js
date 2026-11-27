/* Raft KV Lab: the dashboard page (DESIGN.md M6-C). Plain JS + ECharts, no build step. */
'use strict';

// ------------------------------------------------------------------ palette and helpers

const css = name => getComputedStyle(document.documentElement).getPropertyValue(name).trim();
const C = () => ({
  text: css('--text'), muted: css('--muted'), border: css('--border'), accent: css('--accent'),
  pass: css('--pass'), fail: css('--fail'), warn: css('--warn'), panel: css('--panel'),
});
// Colour-blind-friendly series colours (Okabe-Ito based), in a fixed order so a series keeps its colour.
const SERIES = ['#3b5bdb', '#f08c00', '#2f9e44', '#c2255c', '#1098ad', '#7048e8', '#e8590c', '#5c940d'];

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];
const fmt = (n, d = 0) => n == null || Number.isNaN(n) ? '–'
  : Number(n).toLocaleString('en-US', { maximumFractionDigits: d, minimumFractionDigits: d });
const ms = n => n == null ? '–' : (n >= 100 ? fmt(n, 0) : n >= 10 ? fmt(n, 1) : fmt(n, 2));
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const when = iso => { const d = new Date(iso); return d.toLocaleDateString() + ' ' + d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }); };
const median = xs => { if (!xs.length) return 0; const s = [...xs].sort((a, b) => a - b); const m = s.length >> 1; return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };

async function api(path, opts = {}) {
  const r = await fetch(path, { headers: { 'Content-Type': 'application/json' }, ...opts });
  const body = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(body.error || r.statusText);
  return body;
}

// One ECharts instance per element; disposed when its container is re-rendered.
const charts = new Map();
function chart(el, option) {
  const c = C();
  let inst = charts.get(el);
  if (!inst || inst.isDisposed()) { inst = echarts.init(el, null, { renderer: 'canvas' }); charts.set(el, inst); }
  const base = {
    textStyle: { color: c.text, fontFamily: 'inherit' },
    color: SERIES,
    grid: { left: 56, right: 24, top: 36, bottom: 44, containLabel: false },
    tooltip: { backgroundColor: c.panel, borderColor: c.border, textStyle: { color: c.text }, confine: true },
    legend: { textStyle: { color: c.muted }, top: 0, type: 'scroll' },
    animationDuration: 400,
  };
  const axis = a => a && ({ ...a, axisLine: { lineStyle: { color: c.border } }, axisLabel: { color: c.muted, ...(a.axisLabel || {}) },
    splitLine: { lineStyle: { color: c.border, type: 'dashed' } }, nameTextStyle: { color: c.muted }, nameGap: a.nameGap ?? 30 });
  const fix = o => Array.isArray(o) ? o.map(axis) : axis(o);
  // legend/tooltip are merged, not replaced: a chart that only sets legend.data must keep the theme colours.
  inst.setOption({ ...base, ...option, legend: { ...base.legend, ...(option.legend || {}) },
    tooltip: { ...base.tooltip, ...(option.tooltip || {}) }, xAxis: fix(option.xAxis), yAxis: fix(option.yAxis) }, true);
  return inst;
}
function disposeIn(root) { for (const [el, inst] of charts) if (root.contains(el)) { inst.dispose(); charts.delete(el); } }
window.addEventListener('resize', () => { for (const inst of charts.values()) inst.resize(); });

function card(title, explain, extraHead = '') {
  const el = document.createElement('div');
  el.className = 'card';
  el.innerHTML = `<div class="card-head"><div><h3>${title}</h3><p class="explain">${explain}</p></div>${extraHead}</div><div class="chart"></div>`;
  return el;
}
const kpi = (k, v, d = '', cls = '') => `<div class="kpi ${cls}"><div class="k">${k}</div><div class="v">${v}</div><div class="d">${d}</div></div>`;

// ------------------------------------------------------------------ tabs, segmented controls, job pill

const RESULTS = { sim: '#sim-results', blind: '#bl-results', load: '#load-results', failover: '#fo-results' };
const KIND_NAME = { sim: 'simulation', blind: 'blind bug test', load: 'throughput benchmark', failover: 'failover test' };
$$('.tabs button').forEach(b => b.onclick = () => {
  $$('.tabs button').forEach(x => x.classList.toggle('active', x === b));
  $$('.tab').forEach(t => t.classList.toggle('active', t.id === 'tab-' + b.dataset.tab));
  if (b.dataset.tab === 'compare') loadCompareList();
  else showNewestIfEmpty(b.dataset.tab);
  setTimeout(() => { for (const inst of charts.values()) inst.resize(); }, 30);
});
$$('.seg').forEach(seg => seg.addEventListener('click', e => {
  if (e.target.tagName !== 'BUTTON') return;
  $$('button', seg).forEach(b => b.classList.toggle('on', b === e.target));
  seg.dispatchEvent(new Event('change', { bubbles: true }));
}));
const segValue = (root, name) => $(`.seg[data-name="${name}"] button.on`, root).value;

let options = {};
let runs = [];

const MIX_HELP = {
  chaos: 'The milestone gate mix: message loss, duplicates, delays, one-way partitions, crashes (some mid-fsync), pauses.',
  crashLoop: 'Frequent crashes, most of them in the middle of an fsync: torn WAL tails, recovery again and again.',
  leaderHunting: '70% of faults hit the current leader: the hardest case for ReadIndex reads.',
  quiet: 'No faults at all: a healthy cluster with clients. A baseline.',
  isolateFollower: 'Healthy cluster; at 1 s one follower is cut off for 3 s. Shows what PreVote prevents.',
  isolateLeader: 'Healthy cluster; at 1 s the leader is cut off for 3 s. Shows what CheckQuorum does.',
};
const MUTATION_HELP = {
  none: 'The real protocol. Every run should pass.',
  LYING_DISK: 'fsync says "done" but nothing is saved: votes and entries vanish on a crash.',
  SKIP_DEDUP: 'The state machine runs retried requests again: a retry can undo another client\'s write.',
  SKIP_UP_TO_DATE_CHECK: 'Votes for candidates with stale logs (§5.4.1). Turn PreVote off to see it: PreVote masks it.',
  COMMIT_OLD_TERM_BY_COUNT: 'The Figure 8 bug: commits an old-term entry by counting replicas. Rare under chaos.',
  TRUNCATE_ON_MATCH: 'Deletes matching log entries on AppendEntries: loses committed data.',
  NAIVE_INSTALL_SNAPSHOT: 'Installs stale snapshots and discards the log: rolls a follower back.',
  READ_WITHOUT_QUORUM_CHECK: 'ReadIndex without the heartbeat round: a deposed leader serves stale reads. Try the leaderHunting mix.',
  READ_BEFORE_CURRENT_TERM_COMMIT: 'ReadIndex before the new leader commits its no-op. Try leaderHunting.',
  PREVOTE_BUMPS_TERM: 'PreVote that raises the term like a real election (liveness only: watch the max term).',
  PREVOTE_SKIPS_LOG_CHECK: 'A control: PreVote without the log check is still safe. Should pass.',
  SEND_BEFORE_SYNC: 'Messages leave before the fsync. Use crashLoop + group commit to see it caught.',
  SKIP_TORN_TAIL_CUT: 'Recovery keeps a torn WAL tail. Use crashLoop.',
  SKIP_SNAPSHOT_DATA_FSYNC: 'A snapshot is renamed into place before its bytes are durable. Use crashLoop.',
};

async function init() {
  options = await api('/api/options');
  $('#sim-mix').innerHTML = options.mixes.map(m => `<option value="${m}">${m}</option>`).join('');
  $('#sim-mutation').innerHTML = options.mutations.map(m => `<option value="${m}">${m === 'none' ? 'none (real protocol)' : m}</option>`).join('');
  const help = () => {
    $('#sim-mix-help').textContent = MIX_HELP[$('#sim-mix').value] || '';
    $('#sim-mutation-help').textContent = MUTATION_HELP[$('#sim-mutation').value] || '';
  };
  $('#sim-mix').onchange = help; $('#sim-mutation').onchange = help; help();
  $$('[data-seeds]').forEach(b => b.onclick = () => $('#sim-seeds').value = b.dataset.seeds);
  $$('[data-clients]').forEach(b => b.onclick = () => $('#load-clients').value = b.dataset.clients);
  $$('[data-kills]').forEach(b => b.onclick = () => $('#fo-kills').value = b.dataset.kills);
  initBlindForm();
  $('#load-writes').oninput = () => $('#load-writes-out').textContent = $('#load-writes').value + '%';
  await refreshRuns();
  showNewestIfEmpty('sim');
  poll();
}

// ------------------------------------------------------------------ saved runs

async function refreshRuns() {
  runs = await api('/api/runs');
  for (const ul of $$('.runlist[data-kind]')) {
    const mine = runs.filter(r => r.kind === ul.dataset.kind);
    ul.innerHTML = mine.length ? mine.map(r => `<li data-id="${r.id}"><div class="rl-main"><div class="rl-title">${esc(r.label)}</div>
      <div class="rl-meta">${when(r.createdAt)} · ${headline(r)}</div></div><button class="del" title="Delete">×</button></li>`).join('')
      : '<li class="muted">none yet</li>';
    $$('li[data-id]', ul).forEach(li => {
      li.onclick = e => {
        if (e.target.classList.contains('del')) {
          if (confirm('Delete this saved run?')) api('/api/runs/' + li.dataset.id, { method: 'DELETE' }).then(refreshRuns);
          return;
        }
        $$('li', ul).forEach(x => x.classList.toggle('sel', x === li));
        showRun(li.dataset.id);
      };
    });
  }
  if ($('#tab-compare').classList.contains('active')) loadCompareList();
}

function headline(r) {
  const h = r.headline || {};
  if (r.kind === 'sim') return h.passed === h.seeds ? `<span style="color:var(--pass)">${fmt(h.seeds)} / ${fmt(h.seeds)} passed</span>`
    : `<span style="color:var(--fail)">${fmt(h.seeds - h.passed)} failed</span> of ${fmt(h.seeds)}`;
  if (r.kind === 'load') return `${fmt(h.opsPerSec)} ops/s · p99 ${ms(h.writeP99 || h.readP99)} ms`;
  if (r.kind === 'failover') return `election p50 ${ms(h.electionP50)} ms`;
  if (r.kind === 'blind') return `${fmt(h.caught)}/${fmt(h.planted)} caught · ${h.falseAlarms
    ? `<span style="color:var(--fail)">${fmt(h.falseAlarms)} false alarms</span>` : '0 false alarms'}`;
  return '';
}

/**
 * A tab still showing its "press Run" placeholder opens its newest saved run, so a tab is never blank
 * when there is something to show. Not while that tab's own run is in progress (its live view goes there).
 */
function showNewestIfEmpty(kind) {
  const root = $(RESULTS[kind] || '#none');
  if (!root || !$('.empty', root) || runningKind === kind) return;
  const newest = runs.find(r => r.kind === kind);
  if (!newest) return;
  $$(`.runlist[data-kind="${kind}"] li`).forEach(li => li.classList.toggle('sel', li.dataset.id === newest.id));
  showRun(newest.id);
}

async function showRun(id) {
  const rec = await api('/api/runs/' + id);
  if (rec.kind === 'sim') renderSim(rec);
  if (rec.kind === 'load') renderLoad(rec);
  if (rec.kind === 'failover') renderFailover(rec);
  if (rec.kind === 'blind') renderBlind(rec);
}

// ------------------------------------------------------------------ running jobs

let lastJobState = null;
let runningKind = null;
async function poll() {
  try {
    const j = await api('/api/job');
    runningKind = j.state === 'running' ? j.kind : null;
    const bar = $('#busyBar');
    bar.hidden = j.state !== 'running';
    if (j.state === 'running') {
      bar.innerHTML = `<span>⏳ A ${KIND_NAME[j.kind] || j.kind} is running (${esc(j.label)}) · ${fmt(j.elapsedMs / 1000)} s.
        Runs go one at a time, so the Run buttons unlock when it finishes.</span><button id="busyShow">Show it</button>`;
      $('#busyShow').onclick = () => $(`.tabs button[data-tab="${j.kind}"]`).click();
    }
    const pill = $('#jobPill');
    pill.className = 'pill ' + (j.state === 'running' ? 'running' : 'idle');
    pill.textContent = j.state === 'running' ? `running: ${j.kind} · ${fmt(j.elapsedMs / 1000, 0)} s` : 'idle';
    $$('button.primary').forEach(b => b.disabled = j.state === 'running');
    $('#sim-cancel').hidden = !(j.state === 'running' && j.kind === 'sim');
    if (j.state === 'running') showProgress(j);
    if (lastJobState === 'running' && j.state !== 'running') {
      $$('.progress').forEach(p => p.hidden = true);
      if (j.state === 'failed') alert('Run failed: ' + j.error);
      await refreshRuns();
      if (j.resultId) showRun(j.resultId);
    }
    lastJobState = j.state;
  } catch (e) { /* server restarting */ }
  setTimeout(poll, lastJobState === 'running' ? 500 : 2000);
}

function setProgress(id, frac, text) {
  const p = $(id); p.hidden = false;
  $('.bar', p).style.width = Math.min(100, frac * 100) + '%';
  $('.ptext', p).textContent = text;
}

function showProgress(j) {
  const p = j.progress || {};
  if (j.kind === 'sim') {
    const done = p.done || 0, of = p.of || 1;
    setProgress('#sim-progress', done / of, `${fmt(done)} / ${fmt(of)} seeds · ${fmt(p.failed || 0)} failed`);
  } else if (j.kind === 'blind') {
    const done = p.done || 0, of = p.of || 1;
    setProgress('#bl-progress', done / of, `${fmt(done)} / ${fmt(of)} seeds · ${fmt(p.flagged || 0)} flagged · answer key hidden`);
  } else if (j.kind === 'load') {
    setProgress('#load-progress', (p.second || 0) / (p.of || 1), `${p.phase || 'starting'} · ${p.second || 0} / ${p.of || '?'} s`);
    renderLoadLive(p.live || []);
  } else if (j.kind === 'failover') {
    setProgress('#fo-progress', (p.done || 0) / (p.of || 1), `${p.done || 0} / ${p.of || '?'} kills`);
    renderFailoverLive(p.live || []);
  }
}

/**
 * Starts a run. The results area is cleared only once the server has accepted it; if it refuses (another
 * run in progress, a bad setting) the reason is shown on top of whatever was there.
 */
async function start(kind, body) {
  const root = $(RESULTS[kind]);
  try {
    await api('/api/run/' + kind, { method: 'POST', body: JSON.stringify(body) });
  } catch (e) {
    $('.start-error', root)?.remove();
    root.insertAdjacentHTML('afterbegin', `<div class="banner fail start-error">Could not start: ${esc(e.message)}</div>`);
    return;
  }
  disposeIn(root);
  root.innerHTML = `<div class="empty">Starting the ${KIND_NAME[kind]}…</div>`;
  lastJobState = 'running';
  runningKind = kind;
  $$('button.primary').forEach(b => b.disabled = true);
}

$('#sim-run').onclick = () => start('sim', {
  nodes: +segValue($('#tab-sim'), 'nodes'), seeds: +$('#sim-seeds').value, firstSeed: +$('#sim-first').value,
  mix: $('#sim-mix').value, preVote: $('#sim-prevote').checked, checkQuorum: $('#sim-cq').checked,
  batchWindowMs: $('#sim-batch').checked ? 2 : 0, mutation: $('#sim-mutation').value,
});
$('#goto-blind').onclick = e => { e.preventDefault(); $('.tabs button[data-tab="blind"]').click(); };
$('#sim-cancel').onclick = () => api('/api/job/cancel', { method: 'POST' });
$('#load-run').onclick = () => {
  const root = $('#tab-load');
  start('load', {
    nodes: +segValue(root, 'nodes'), fsyncBatching: $('#load-batch').checked, clients: +$('#load-clients').value,
    writePercent: +$('#load-writes').value, keys: +$('#load-keys').value, valueBytes: +$('#load-value').value,
    warmupSec: +$('#load-warmup').value, durationSec: +$('#load-duration').value,
  });
};
$('#fo-run').onclick = () => {
  start('failover', { nodes: +segValue($('#tab-failover'), 'nodes'), fsyncBatching: $('#fo-batch').checked, kills: +$('#fo-kills').value });
};

// ------------------------------------------------------------------ simulator results

const CHECK_WHAT = {
  'Election Safety': 'at most one leader per term',
  'Vote durability': 'a vote survives a crash',
  'Leader Append-Only': 'a leader never rewrites its log',
  'Log Matching': 'same index + term → same prefix',
  'Leader Completeness': 'new leaders have every committed entry',
  'State Machine Safety': 'no two nodes apply different commands at an index',
  'Replica state divergence': 'replicas with the same applied index agree',
  'Durability': 'no committed or fsynced data lost in a crash',
  'Linearizability': 'clients saw one correct, up-to-date order',
  'Liveness': 'after faults heal, leaders and clients make progress',
};
const SIM_METRICS = {
  elections: 'Elections', maxTerm: 'Highest term', crashes: 'Crashes', midWriteCrashes: 'Crashes during fsync',
  partitions: 'Partitions', ops: 'Client operations', writes: 'Writes completed', reads: 'Reads completed',
  dedupHits: 'Retries answered from the session table', timeouts: 'Client timeouts', linSteps: 'Linearizability search steps',
  maxCommitIndex: 'Commit index reached', snapshotsTaken: 'Snapshots taken', slowestClientIdleMs: 'Slowest client idle at end (ms)',
  newLeadersAfterScenario: 'New leaders after the scripted fault', stepDownMs: 'Old leader stepped down after (ms)', avgBatch: 'Average batch size',
};

function renderSim(rec) {
  const root = $('#sim-results');
  disposeIn(root);
  const r = rec.result, s = rec.settings;
  const failed = r.seeds - r.passed;
  const t = r.totals;
  const scenario = s.mix.startsWith('isolate');
  root.innerHTML = `
    <div class="title-row"><h2>${esc(rec.label)}</h2><span class="meta">${when(rec.createdAt)} · commit ${esc(rec.gitCommit)} · ${fmt(r.wallMs / 1000, 1)} s</span></div>
    <div class="banner ${failed ? 'fail' : 'pass'}">${failed
      ? `✗ ${fmt(failed)} of ${fmt(r.seeds)} seeds broke a property${s.mutation !== 'none' ? ': the deliberate bug was caught' : ''}`
      : `✓ All ${fmt(r.seeds)} seeds passed every safety and liveness check${s.mutation !== 'none' ? ' (this mutation was not caught by these runs)' : ''}`}</div>
    <div class="kpis">
      ${kpi('Seeds passed', `${fmt(r.passed)}<small>/ ${fmt(r.seeds)}</small>`, `${fmt(100 * r.passed / r.seeds, 1)}%`, failed ? 'bad' : 'good')}
      ${kpi('Elections won', fmt(t.elections), `${fmt(t.elections / r.seeds, 1)} per run`)}
      ${kpi('Highest term', fmt(t.maxTerm), 'lower = fewer wasted elections')}
      ${kpi('Crashes', fmt(t.crashes), `${fmt(t.midWriteCrashes)} in the middle of an fsync`)}
      ${kpi('Client operations', fmt(t.ops), `${fmt(t.writes)} writes · ${fmt(t.reads)} reads`)}
      ${kpi('Retries deduplicated', fmt(t.dedupHits), 'answered from the session table')}
      ${kpi('Simulated events', fmt(t.events), `${fmt(t.events / (r.wallMs / 1000))} per second`)}
    </div>
    <div class="card"><h3>Safety and liveness checks</h3><p class="explain">Each property is checked after every single event of every run (liveness at the end). A red box shows how many seeds broke it.</p>
      <div class="checks-grid">${options.checkers.map(name => {
        const n = r.outcomes[name] || 0;
        return `<div class="check ${n ? 'bad' : 'ok'}"><div class="icon">${n ? '✗' : '✓'}</div><div><div class="name">${name}${n ? ` · ${fmt(n)} seeds` : ''}</div><div class="what">${CHECK_WHAT[name] || ''}</div></div></div>`;
      }).join('')}</div></div>
    <div class="row2" id="sim-row1"></div>
    <div id="sim-scatter"></div>
    <div class="card"><div class="card-head"><div><h3>Every seed</h3><p class="explain">Failing seeds first. Click a row to see what broke and the last events before it.</p></div></div>
      <div class="tablewrap"><table class="data" id="sim-table"></table></div></div>`;

  // outcome donut
  const row = $('#sim-row1');
  const donut = card('Outcome per seed', 'Passed, or which checker caught the run first.');
  row.appendChild(donut);
  const c = C();
  const outcomeData = Object.entries(r.outcomes).map(([k, v]) => ({ name: k, value: v, itemStyle: { color: k === 'passed' ? c.pass : undefined } }));
  chart($('.chart', donut), {
    tooltip: { trigger: 'item', formatter: p => `${p.name}<br><b>${fmt(p.value)}</b> seeds (${fmt(p.percent, 1)}%)` },
    legend: { bottom: 0, top: 'auto', textStyle: { color: c.muted } },
    color: [c.fail, '#c2255c', c.warn, '#7048e8', '#1098ad', '#e8590c'],
    series: [{ type: 'pie', radius: ['48%', '72%'], center: ['50%', '45%'], data: outcomeData,
      label: { color: c.text, formatter: p => `${p.name}\n${fmt(p.value)}` }, itemStyle: { borderColor: c.panel, borderWidth: 2 } }],
  });

  // distribution histogram with metric picker
  const metricKeys = Object.keys(SIM_METRICS).filter(k => scenario || !['newLeadersAfterScenario', 'stepDownMs'].includes(k))
    .filter(k => k !== 'avgBatch' || s.batchWindowMs > 0);
  const pick = `<select class="metric">${metricKeys.map(k => `<option value="${k}" ${k === (scenario ? 'newLeadersAfterScenario' : 'elections') ? 'selected' : ''}>${SIM_METRICS[k]}</option>`).join('')}</select>`;
  const hist = card('Distribution across seeds', 'How a number varies from run to run. Hover a bar for the exact range and count.', pick);
  row.appendChild(hist);
  const passedRows = r.rows.filter(x => x.outcome === 'passed');
  const drawHist = key => {
    const vals = passedRows.map(x => x[key]).filter(v => v != null && v >= 0);
    const { labels, counts } = bins(vals, 24);
    chart($('.chart', hist), {
      tooltip: { trigger: 'axis', formatter: ps => `${SIM_METRICS[key]}: <b>${ps[0].name}</b><br>${fmt(ps[0].value)} seeds` },
      xAxis: { type: 'category', data: labels, name: SIM_METRICS[key], nameLocation: 'middle', axisLabel: { hideOverlap: true } },
      yAxis: { type: 'value', name: 'seeds' },
      series: [{ type: 'bar', data: counts, itemStyle: { color: c.accent, borderRadius: [4, 4, 0, 0] }, barCategoryGap: '8%' }],
    });
  };
  $('select.metric', hist).onchange = e => drawHist(e.target.value);
  drawHist($('select.metric', hist).value);

  // scatter: every seed
  const sc = card('Every run at a glance', 'One dot per seed. Green passed, red failed. Click a dot to inspect that seed.',
    `<select class="metric">${metricKeys.map(k => `<option value="${k}" ${k === 'ops' ? 'selected' : ''}>${SIM_METRICS[k]}</option>`).join('')}</select>`);
  $('#sim-scatter').appendChild(sc);
  const drawScatter = key => {
    const inst = chart($('.chart', sc), {
      tooltip: { trigger: 'item', formatter: p => `seed <b>${p.value[0]}</b><br>${SIM_METRICS[key]}: ${fmt(p.value[1], 2)}<br>${esc(p.value[2])}` },
      legend: { data: ['passed', 'failed'] },
      dataZoom: [{ type: 'inside' }, { type: 'slider', height: 18, bottom: 6 }],
      grid: { left: 56, right: 24, top: 36, bottom: 64 },
      xAxis: { type: 'value', name: 'seed', nameLocation: 'middle', min: 'dataMin', max: 'dataMax' },
      yAxis: { type: 'value', name: SIM_METRICS[key] },
      series: [
        { name: 'passed', type: 'scatter', symbolSize: 6, itemStyle: { color: c.pass, opacity: .7 }, large: true,
          data: passedRows.map(x => [x.seed, x[key], 'passed']) },
        { name: 'failed', type: 'scatter', symbolSize: 11, itemStyle: { color: c.fail },
          data: r.rows.filter(x => x.outcome !== 'passed').map(x => [x.seed, 0, x.outcome]) },
      ],
    });
    inst.off('click');
    inst.on('click', p => seedDrawer(r.rows.find(x => x.seed === p.value[0]), s));
  };
  $('select.metric', sc).onchange = e => drawScatter(e.target.value);
  drawScatter('ops');

  // table
  const cols = [['seed', 'Seed'], ['outcome', 'Outcome'], ['elections', 'Elections'], ['maxTerm', 'Max term'], ['crashes', 'Crashes'],
    ['midWriteCrashes', 'Mid-fsync'], ['partitions', 'Partitions'], ['ops', 'Client ops'], ['dedupHits', 'Dedup hits'],
    ['timeouts', 'Timeouts'], ['linSteps', 'Lin. steps'], ['maxCommitIndex', 'Commit idx']];
  let sortKey = 'outcome', asc = true;
  const drawTable = () => {
    const rows = [...r.rows].sort((a, b) => {
      if (sortKey === 'outcome') return (a.outcome === 'passed') - (b.outcome === 'passed') || a.seed - b.seed;
      return (asc ? 1 : -1) * ((a[sortKey] ?? 0) - (b[sortKey] ?? 0));
    }).slice(0, 1000);
    $('#sim-table').innerHTML = `<thead><tr>${cols.map(([k, l]) => `<th data-k="${k}">${l}${sortKey === k ? (asc ? ' ▲' : ' ▼') : ''}</th>`).join('')}</tr></thead>
      <tbody>${rows.map(x => `<tr class="click" data-seed="${x.seed}">${cols.map(([k]) => `<td>${k === 'outcome'
        ? `<span class="tag ${x.outcome === 'passed' ? 'pass' : 'fail'}">${esc(x.outcome)}</span>` : fmt(x[k])}</td>`).join('')}</tr>`).join('')}</tbody>`;
    $$('#sim-table th').forEach(th => th.onclick = () => { asc = sortKey === th.dataset.k ? !asc : false; sortKey = th.dataset.k; drawTable(); });
    $$('#sim-table tr.click').forEach(tr => tr.onclick = () => seedDrawer(r.rows.find(x => x.seed === +tr.dataset.seed), s));
  };
  drawTable();
}

function bins(vals, n) {
  if (!vals.length) return { labels: [], counts: [] };
  const lo = Math.min(...vals), hi = Math.max(...vals);
  const ints = vals.every(Number.isInteger);
  if (ints && hi - lo + 1 <= n) {
    const labels = [], counts = [];
    for (let v = lo; v <= hi; v++) { labels.push(String(v)); counts.push(0); }
    for (const v of vals) counts[v - lo]++;
    return { labels, counts };
  }
  const w = (hi - lo) / n || 1;
  const counts = new Array(n).fill(0);
  for (const v of vals) counts[Math.min(n - 1, Math.floor((v - lo) / w))]++;
  const labels = counts.map((_, i) => `${fmt(lo + i * w, ints ? 0 : 1)}–${fmt(lo + (i + 1) * w, ints ? 0 : 1)}`);
  return { labels, counts };
}

function seedDrawer(row, settings) {
  if (!row) return;
  const ok = row.outcome === 'passed';
  $('#drawer-body').innerHTML = `<h2>Seed ${row.seed} <span class="tag ${ok ? 'pass' : 'fail'}">${esc(row.outcome)}</span></h2>
    <p class="hint">${esc(settings ? JSON.stringify(settings) : '')}</p>
    ${ok ? `<div class="kpis">${Object.entries(SIM_METRICS).filter(([k]) => row[k] != null && row[k] >= 0)
      .map(([k, l]) => kpi(l, fmt(row[k], k === 'avgBatch' ? 2 : 0))).join('')}</div>`
    : `<p>This run broke <b>${esc(row.outcome)}</b>. Below: the checker's message, then the simulator's last events before the failure (newest at the bottom). The same seed with the same settings always replays exactly.</p>
       <pre class="trace">${esc(row.message)}</pre>`}
    <div class="actions"><button class="primary" id="replay">Run this seed again</button></div>`;
  $('#drawer').hidden = false;
  $('#replay').onclick = () => { $('#drawer').hidden = true; start('sim', { ...settings, seeds: 1, firstSeed: row.seed }); };
}
$('#drawer-close').onclick = () => $('#drawer').hidden = true;
$('#drawer').onclick = e => { if (e.target.id === 'drawer') $('#drawer').hidden = true; };

// ------------------------------------------------------------------ blind bug test

/** The bug picker: tick a bug to plant it, then say in how many seeds. Nothing is ticked at first. */
function initBlindForm() {
  const box = $('#bl-bugs');
  box.innerHTML = options.bugs.map(b => `<label class="bugrow" data-bug="${b.name}">
      <input type="checkbox" class="pick">
      <div><div class="bn">${b.name}</div><div class="bw">${esc(b.what)}<i class="ru"><br>runs under: ${esc(b.profile)}</i></div></div>
      <input type="number" class="count" min="1" value="10" disabled title="how many seeds get this bug">
      <div class="warn"></div></label>`).join('');
  $('#bl-mix').innerHTML = options.mixes.map(m => `<option value="${m}">${m}</option>`).join('');
  const sum = () => {
    let n = 0, types = 0;
    $$('.bugrow', box).forEach(r => {
      const on = $('.pick', r).checked;
      $('.count', r).disabled = !on;
      r.classList.toggle('on', on);
      if (on) { n += Math.max(0, +$('.count', r).value || 0); types++; }
    });
    const total = +$('#bl-seeds').value || 0;
    $('#bl-sum').innerHTML = !types ? '<span style="color:var(--warn)">Tick at least one bug.</span>'
      : n > total ? `<span style="color:var(--fail)">${fmt(n)} bugs do not fit in ${fmt(total)} seeds</span>`
      : `<b>${types}</b> bug type${types > 1 ? 's' : ''}, <b>${fmt(n)}</b> bugged seeds, ${fmt(total - n)} clean (${fmt(total ? 100 * (total - n) / total : 0)}%).`;
    $('#bl-run').dataset.ok = types && n <= total ? '1' : '';
    const custom = blindCustom();
    $('#bl-custom').classList.toggle('dim', !custom);
    box.classList.toggle('custom', !!custom);
    $$('#bl-custom select, #bl-custom input').forEach(i => i.disabled = !custom);
    $('#bl-mode-help').textContent = custom
      ? 'Every seed, bugged or clean, uses the settings below. Some bugs cannot show under some settings: see the warnings on ticked bugs.'
      : 'Each bug runs under the conditions that can expose it (shown as "runs under"). Clean seeds get a random mix of those, so the settings do not give the plan away.';
    $('#bl-mix-help').textContent = MIX_HELP[$('#bl-mix').value] || '';
    $$('.bugrow', box).forEach(r => $('.warn', r).textContent = custom && $('.pick', r).checked ? maskWarning(r.dataset.bug, custom) : '');
  };
  $('#tab-blind').addEventListener('change', sum);
  box.addEventListener('input', sum);
  box.addEventListener('change', sum);
  $('#bl-seeds').addEventListener('input', sum);
  $('#bl-each10').textContent = 'select all';
  $('#bl-each10').onclick = () => { $$('.bugrow', box).forEach(r => $('.pick', r).checked = true); sum(); };
  $('#bl-clear').onclick = () => { $$('.bugrow', box).forEach(r => { $('.pick', r).checked = false; $('.count', r).value = 10; }); sum(); };
  $('#bl-random').onclick = () => {
    const per = Math.max(1, Math.floor((+$('#bl-seeds').value || 0) * 0.3 / options.bugs.length));
    $$('.bugrow', box).forEach(r => { const on = Math.random() < 0.6; $('.pick', r).checked = on; $('.count', r).value = 1 + Math.floor(Math.random() * per * 2); });
    sum();
  };
  sum();
}

/** The fixed settings, or null when each bug uses its own. */
function blindCustom() {
  if (segValue($('#tab-blind'), 'blmode') !== 'custom') return null;
  return { mix: $('#bl-mix').value, preVote: $('#bl-prevote').checked, checkQuorum: $('#bl-cq').checked, batchWindowMs: $('#bl-batch').checked ? 2 : 0 };
}

/** Why a bug may never show under the chosen settings (from the mutation tests' findings). */
function maskWarning(bug, s) {
  if (s.mix === 'quiet') return '⚠ quiet has no faults: almost no bug can show without crashes or partitions.';
  if (s.mix.startsWith('isolate')) return '⚠ one scripted partition, no crashes: most bugs need more faults to show.';
  switch (bug) {
    case 'SKIP_UP_TO_DATE_CHECK': return s.preVote ? '⚠ PreVote on hides this bug (PreVote checks logs too). Turn PreVote off.' : '';
    case 'COMMIT_OLD_TERM_BY_COUNT': return '⚠ very rare here: its own settings also turn hardening off and send 1 entry per AppendEntries.';
    case 'SEND_BEFORE_SYNC': return s.mix !== 'crashLoop' || s.batchWindowMs === 0
      ? 'tip: needs a crash between a send and its fsync; shows most under crashLoop with Group commit on.' : '';
    case 'SKIP_TORN_TAIL_CUT': case 'SKIP_SNAPSHOT_DATA_FSYNC':
      return s.mix !== 'crashLoop' ? 'tip: needs crashes in the middle of a write; shows most under crashLoop.' : '';
    case 'READ_WITHOUT_QUORUM_CHECK': return s.mix !== 'leaderHunting' ? 'tip: needs a deposed leader still serving reads; shows most under leaderHunting with CheckQuorum off.'
      : s.checkQuorum ? 'tip: CheckQuorum narrows the window this bug needs (still possible).' : '';
    case 'READ_BEFORE_CURRENT_TERM_COMMIT': return s.mix !== 'leaderHunting' ? 'tip: needs a read right after a leader change; shows most under leaderHunting.' : '';
    default: return '';
  }
}

$('#bl-run').onclick = () => {
  if (!$('#bl-run').dataset.ok) { $('#bl-sum').scrollIntoView({ block: 'center' }); return; }
  const bugs = {};
  $$('#bl-bugs .bugrow').forEach(r => { if ($('.pick', r).checked) bugs[r.dataset.bug] = +$('.count', r).value || 0; });
  start('blind', { nodes: +segValue($('#tab-blind'), 'nodes'), seeds: +$('#bl-seeds').value, firstSeed: +$('#bl-first').value,
    planSeed: +$('#bl-plan').value || 0, bugs, settings: blindCustom() });
};

const SEED_COLS = [['elections', 'Elections'], ['maxTerm', 'Max term'], ['crashes', 'Crashes'], ['midWriteCrashes', 'Mid-fsync'],
  ['partitions', 'Partitions'], ['ops', 'Client ops'], ['dedupHits', 'Dedup hits'], ['timeouts', 'Timeouts'],
  ['linSteps', 'Lin. steps'], ['maxCommitIndex', 'Commit idx']];
const secs = msv => msv == null || msv < 0 ? '–' : fmt(msv / 1000, 2) + ' s';

function renderBlind(rec) {
  const root = $('#bl-results');
  disposeIn(root);
  const r = rec.result, c = C();
  const rate = r.planted ? r.caught / r.planted : 0;
  const allGood = r.missed === 0 && r.falseAlarms === 0;
  const bugs = [...r.perBug].sort((a, b) => b.caught / b.planted - a.caught / a.planted || a.bug.localeCompare(b.bug));
  root.innerHTML = `
    <div class="title-row"><h2>${esc(rec.label)}</h2><span class="meta">${when(rec.createdAt)} · plan seed ${r.planSeed} · ${fmt(r.wallMs / 1000, 1)} s</span></div>
    <div class="banner ${r.falseAlarms ? 'fail' : allGood ? 'pass' : 'info'}">${r.falseAlarms
      ? `✗ ${fmt(r.falseAlarms)} clean seed(s) were flagged: a checker (or the protocol) is wrong. Open them below.`
      : allGood ? `✓ Every planted bug was caught, and no clean seed was flagged.`
      : `✓ No false alarms. ${fmt(r.caught)} of ${fmt(r.planted)} planted bugs were caught; ${fmt(r.missed)} went unnoticed in their run (see why below).`}</div>
    <div class="kpis">
      ${kpi('Detection rate', `${fmt(100 * rate, 1)}<small>%</small>`, `${fmt(r.caught)} of ${fmt(r.planted)} bugged seeds flagged`, rate > .8 ? 'good' : '')}
      ${kpi('Bug types', fmt(bugs.length), `${bugs.filter(b => b.caught === b.planted).length} caught every time`)}
      ${kpi('False alarms', fmt(r.falseAlarms), `of ${fmt(r.clean)} clean seeds`, r.falseAlarms ? 'bad' : 'good')}
      ${kpi('Missed', fmt(r.missed), 'bug present, run looked fine')}
      ${kpi('Seeds', fmt(r.seeds), `${fmt(r.planted)} bugged · ${fmt(r.clean)} clean`)}
    </div>
    <div class="card"><h3>The answer key</h3><p class="explain">Rows: what was really planted. Columns: what the checkers said. Green cells are right answers.</p>
      <table class="matrix"><tr><th></th><th>flagged by a checker</th><th>not flagged</th></tr>
        <tr><th class="rowh">bug planted</th><td class="good"><div class="n">${fmt(r.caught)}</div><div class="l">caught</div></td>
            <td class="meh"><div class="n">${fmt(r.missed)}</div><div class="l">missed</div></td></tr>
        <tr><th class="rowh">clean</th><td class="${r.falseAlarms ? 'bad' : 'good'}"><div class="n">${fmt(r.falseAlarms)}</div><div class="l">false alarm</div></td>
            <td class="good"><div class="n">${fmt(r.cleanPasses)}</div><div class="l">clean pass</div></td></tr></table>
      <p class="explain" style="margin-top:12px">A <b>false alarm</b> would be serious: a checker flags correct behaviour, or the real protocol has a bug.
      A <b>miss</b> is usually not a weak checker: a bug can only be seen if the run happens to hit the situation it breaks
      (the Figure 8 bug needs a leader to crash at one exact moment). The checkers judge what happened, not what the code is.</p></div>
    <div class="card"><h3>Each planted bug type</h3><p class="explain">Click a row to list only that bug's seeds below. "Caught at" is the simulated time into the 13 s run when the checker fired. Most checkers run after every event; Linearizability looks at the whole client history once, at the end, so its bugs always show 13 s.</p>
      <div class="tablewrap"><table class="data" id="bl-bugtable"><thead><tr><th>Bug</th><th>What it breaks</th><th>Runs under</th>
        <th>Planted</th><th>Caught</th><th>Missed</th><th style="min-width:130px">Detection</th><th>Caught by</th><th>Caught at (first · median · last)</th></tr></thead>
      <tbody>${bugs.map(b => { const pct = 100 * b.caught / b.planted; return `<tr class="click" data-bug="${b.bug}">
        <td><code>${b.bug}</code></td><td>${esc(b.what)}</td><td>${esc(b.profile)}</td><td>${b.planted}</td>
        <td style="color:var(--pass);font-weight:700">${b.caught}</td><td style="color:${b.planted - b.caught ? 'var(--warn)' : 'var(--muted)'};font-weight:700">${b.planted - b.caught}</td>
        <td><div class="minibar"><span style="width:${pct}%"></span></div>${fmt(pct)}%</td>
        <td>${Object.entries(b.caughtBy).map(([k, v]) => `${esc(k)} <b>${v}</b>`).join('<br>') || '–'}</td>
        <td>${b.caught ? `${secs(b.firstCaughtAtMs)} · ${secs(b.medianCaughtAtMs)} · ${secs(b.lastCaughtAtMs)}` : '–'}</td></tr>`; }).join('')}</tbody></table></div></div>
    <div class="row2" id="bl-row"></div>
    <div class="card"><div class="card-head"><div><h3>Every seed</h3><p class="explain">Click a column to sort, a row for the checker's message. For a flagged seed the numbers cover the run up to the moment it was caught.</p></div>
      <div class="filters"><select id="bl-bugfilter"><option value="">all bugs</option><option value="none">clean seeds</option>${bugs.map(b => `<option value="${b.bug}">${b.bug}</option>`).join('')}</select></div></div>
      <div class="filters" id="bl-filter" style="margin-bottom:8px">${['all', 'caught', 'missed', 'false alarm', 'clean pass'].map((f, i) => `<button class="${i ? '' : 'on'}" value="${f}">${f}</button>`).join('')}</div>
      <div class="tablewrap"><table class="data" id="bl-table"></table></div><p class="hint" id="bl-count"></p></div>`;

  // per-bug detection bars
  const bars = card('Detection rate per bug', 'Green: seeds where the bug was caught. Grey: seeds where it was present but the run never hit it.');
  $('#bl-row').appendChild(bars);
  $('.chart', bars).style.height = Math.max(240, 46 * bugs.length + 60) + 'px';
  chart($('.chart', bars), {
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }, formatter: ps => {
      const b = bugs[ps[0].dataIndex];
      return `<b>${b.bug}</b><br>${esc(b.what)}<br>caught ${b.caught} of ${b.planted} (${fmt(100 * b.caught / b.planted, 0)}%)<br>`
        + Object.entries(b.caughtBy).map(([k, v]) => `&nbsp;· ${k}: ${v}`).join('<br>');
    } },
    legend: { data: ['caught', 'missed'] },
    grid: { left: 230, right: 50, top: 32, bottom: 30 },
    xAxis: { type: 'value', name: 'seeds', max: 'dataMax', minInterval: 1 },
    yAxis: { type: 'category', data: bugs.map(b => b.bug), inverse: true, axisLabel: { fontSize: 11, fontFamily: 'monospace' } },
    series: [
      { name: 'caught', type: 'bar', stack: 's', data: bugs.map(b => b.caught), itemStyle: { color: c.pass },
        label: { show: true, position: 'insideLeft', color: '#fff', fontWeight: 700, formatter: p => `${fmt(100 * bugs[p.dataIndex].caught / bugs[p.dataIndex].planted, 0)}%` } },
      { name: 'missed', type: 'bar', stack: 's', data: bugs.map(b => b.planted - b.caught), itemStyle: { color: c.border } },
    ],
  });

  // which checker caught which bug
  const checkers = [...new Set(bugs.flatMap(b => Object.keys(b.caughtBy)))].sort();
  const heat = card('Which checker caught which bug', 'Each cell: seeds of that bug first flagged by that checker. Different bugs break different properties.');
  $('#bl-row').appendChild(heat);
  $('.chart', heat).style.height = Math.max(240, 46 * bugs.length + 90) + 'px';
  const cells = [];
  bugs.forEach((b, y) => checkers.forEach((k, x) => cells.push([x, y, b.caughtBy[k] || 0])));
  chart($('.chart', heat), {
    tooltip: { formatter: p => `${bugs[p.value[1]].bug}<br>${checkers[p.value[0]]}: <b>${p.value[2]}</b> seeds` },
    grid: { left: 230, right: 20, top: 20, bottom: 100 },
    xAxis: { type: 'category', data: checkers, axisLabel: { rotate: 30, fontSize: 11, interval: 0 }, splitArea: { show: true } },
    yAxis: { type: 'category', data: bugs.map(b => b.bug), inverse: true, axisLabel: { fontSize: 11, fontFamily: 'monospace' } },
    visualMap: { show: false, min: 0, max: Math.max(1, ...cells.map(x => x[2])), inRange: { color: [c.panel, c.accent] } },
    series: [{ type: 'heatmap', data: cells, label: { show: true, color: c.text, formatter: p => p.value[2] || '' },
      itemStyle: { borderColor: c.border, borderWidth: 1 } }],
  });

  // seed table: filters + sortable columns
  const tagCls = v => v === 'caught' || v === 'clean pass' ? 'pass' : v === 'missed' ? 'miss' : 'fail';
  const order = { 'false alarm': 0, missed: 1, caught: 2, 'clean pass': 3 };
  const cols = [['seed', 'Seed'], ['bug', 'Planted bug'], ['verdict', 'Verdict'], ['outcome', 'Checker'], ...SEED_COLS, ['failedAtMs', 'Caught at']];
  const val = (x, k) => ['seed', 'bug', 'verdict', 'outcome'].includes(k) ? x[k] : x.stats[k];
  let verdict = 'all', bugF = '', sortKey = 'verdict', asc = true;
  const draw = () => {
    const rows = r.rows.filter(x => (verdict === 'all' || x.verdict === verdict) && (!bugF || x.bug === bugF)).sort((a, b) => {
      if (sortKey === 'verdict') return (asc ? 1 : -1) * (order[a.verdict] - order[b.verdict]) || a.seed - b.seed;
      const va = val(a, sortKey), vb = val(b, sortKey);
      const d = typeof va === 'string' ? va.localeCompare(vb) : (va ?? 0) - (vb ?? 0);
      return (asc ? 1 : -1) * d || a.seed - b.seed;
    });
    const shown = rows.slice(0, 2000);
    $('#bl-table').innerHTML = `<thead><tr>${cols.map(([k, l]) => `<th data-k="${k}">${l}${sortKey === k ? (asc ? ' ▲' : ' ▼') : ''}</th>`).join('')}</tr></thead>
      <tbody>${shown.map(x => `<tr class="click" data-seed="${x.seed}"><td>${x.seed}</td><td>${x.bug === 'none' ? '<span class="muted">none</span>' : `<code>${x.bug}</code>`}</td>
        <td><span class="tag ${tagCls(x.verdict)}">${x.verdict}</span></td><td>${x.flagged ? esc(x.outcome) : '<span class="muted">passed</span>'}</td>
        ${SEED_COLS.map(([k]) => `<td>${fmt(x.stats[k])}</td>`).join('')}<td>${x.flagged ? secs(x.stats.failedAtMs) : '–'}</td></tr>`).join('')}</tbody>`;
    $('#bl-count').textContent = `${fmt(rows.length)} seeds${rows.length > shown.length ? `, first ${fmt(shown.length)} shown` : ''}.`;
    $$('#bl-table th').forEach(th => th.onclick = () => { asc = sortKey === th.dataset.k ? !asc : !['seed', 'bug', 'verdict', 'outcome'].includes(th.dataset.k) ? false : true; sortKey = th.dataset.k; draw(); });
    $$('#bl-table tr.click').forEach(tr => tr.onclick = () => blindDrawer(r.rows.find(x => x.seed === +tr.dataset.seed)));
  };
  $$('#bl-filter button').forEach(b => b.onclick = () => { verdict = b.value; $$('#bl-filter button').forEach(x => x.classList.toggle('on', x === b)); draw(); });
  $('#bl-bugfilter').onchange = e => { bugF = e.target.value; draw(); };
  $$('#bl-bugtable tr.click').forEach(tr => tr.onclick = () => {
    bugF = tr.dataset.bug; $('#bl-bugfilter').value = bugF; draw();
    $('#bl-table').scrollIntoView({ behavior: 'smooth', block: 'start' });
  });
  draw();
}

function blindDrawer(row) {
  const bug = options.bugs.find(b => b.name === row.bug);
  const st = row.stats;
  const explain = {
    caught: `The planted bug <b>${row.bug}</b> broke <b>${esc(row.outcome)}</b> after ${secs(st.failedAtMs)} of simulated time, and the checker stopped the run.`,
    missed: `<b>${row.bug}</b> was active for the whole run, but the run never reached the situation it breaks, so every invariant still held.`,
    'false alarm': `No bug was planted, yet <b>${esc(row.outcome)}</b> failed. Either the checker is wrong or the real protocol has a bug. Investigate this seed.`,
    'clean pass': 'No bug planted and every check passed, as it should.',
  }[row.verdict];
  $('#drawer-body').innerHTML = `<h2>Seed ${row.seed} <span class="tag ${row.verdict === 'caught' || row.verdict === 'clean pass' ? 'pass' : row.verdict === 'missed' ? 'miss' : 'fail'}">${row.verdict}</span></h2>
    <p><b>Planted:</b> <code>${row.bug}</code>${bug ? ` (${esc(bug.what)})` : ''}<br><b>Settings:</b> ${esc(row.profile)}</p>
    <p>${explain}</p>
    <div class="kpis">${SEED_COLS.map(([k, l]) => kpi(l, fmt(st[k]))).join('')}${kpi('Simulated events', fmt(st.events))}</div>
    ${row.flagged ? `<h3 style="margin-top:18px">Checker message and the last events before it</h3><pre class="trace">${esc(row.message)}</pre>` : ''}`;
  $('#drawer').hidden = false;
}

// ------------------------------------------------------------------ load results

function renderLoadLive(live) {
  const root = $('#load-results');
  let el = $('#load-live', root);
  if (!el && !live.length) return;   // keep "Starting…" until the first measured second
  if (!el) {
    disposeIn(root);
    root.innerHTML = '';
    el = card('Live throughput', 'Operations completed each second while the benchmark runs (after warm-up). The dashed line is the p99 latency.');
    el.id = 'load-live';
    root.appendChild(el);
  }
  const c = C();
  chart($('.chart', el), {
    tooltip: { trigger: 'axis' },
    legend: { data: ['ops/s', 'p99 ms'] },
    xAxis: { type: 'category', data: live.map(x => x.second), name: 'second', nameLocation: 'middle' },
    yAxis: [{ type: 'value', name: 'ops/s' }, { type: 'value', name: 'p99 ms', splitLine: { show: false } }],
    series: [{ name: 'ops/s', type: 'line', smooth: true, areaStyle: { opacity: .15 }, data: live.map(x => x.ops), itemStyle: { color: c.accent } },
      { name: 'p99 ms', type: 'line', yAxisIndex: 1, lineStyle: { type: 'dashed' }, data: live.map(x => x.p99), itemStyle: { color: SERIES[1] } }],
  });
}

function renderLoad(rec) {
  const root = $('#load-results');
  disposeIn(root);
  const r = rec.result, s = rec.settings;
  const hasW = r.writes.count > 0, hasR = r.reads.count > 0;
  root.innerHTML = `
    <div class="title-row"><h2>${esc(rec.label)}</h2><span class="meta">${when(rec.createdAt)} · commit ${esc(rec.gitCommit)} · ${esc(rec.machine?.os || '')}, ${esc(rec.machine?.cpus || '?')} CPUs</span></div>
    <div class="kpis">
      ${kpi('Throughput', `${fmt(r.opsPerSec)}<small>ops/s</small>`, `${fmt(r.ops)} operations in ${s.durationSec} s`)}
      ${hasW ? kpi('Write latency p50', `${ms(r.writes.p50Ms)}<small>ms</small>`, `p99 ${ms(r.writes.p99Ms)} · max ${ms(r.writes.maxMs)} ms`) : ''}
      ${hasR ? kpi('Read latency p50', `${ms(r.reads.p50Ms)}<small>ms</small>`, `p99 ${ms(r.reads.p99Ms)} · max ${ms(r.reads.maxMs)} ms`) : ''}
      ${kpi('fsync time (leader)', `${ms(r.server.fsyncMeanMs)}<small>ms</small>`, `mean · p99 ${ms(r.server.fsyncP99Ms)} ms · ${fmt(r.server.fsyncs)} fsyncs`)}
      ${kpi('Events per fsync', fmt(r.server.batchMean, 1), s.fsyncBatching ? 'group commit on: one fsync covers this many events' : 'group commit off: one fsync per event')}
      ${kpi('Client timeouts', fmt(r.timeouts), 'operations given up after 10 s', r.timeouts ? 'bad' : 'good')}
    </div>
    <div id="load-c1"></div><div class="row2" id="load-c2"></div>`;
  const c = C();
  const t = card('Throughput and tail latency over time', 'Each point is one second of the measured period. Flat lines mean a steady system; spikes show stalls (GC, a slow fsync).');
  $('#load-c1').appendChild(t);
  chart($('.chart', t), {
    tooltip: { trigger: 'axis' }, legend: { data: ['ops/s', 'p50 ms', 'p99 ms'] },
    xAxis: { type: 'category', data: r.perSecond.map(x => x.second), name: 'second', nameLocation: 'middle' },
    yAxis: [{ type: 'value', name: 'ops/s' }, { type: 'value', name: 'latency ms', splitLine: { show: false } }],
    series: [{ name: 'ops/s', type: 'bar', data: r.perSecond.map(x => x.ops), itemStyle: { color: c.accent, opacity: .8, borderRadius: [3, 3, 0, 0] } },
      { name: 'p50 ms', type: 'line', yAxisIndex: 1, smooth: true, data: r.perSecond.map(x => x.p50Ms), itemStyle: { color: SERIES[2] } },
      { name: 'p99 ms', type: 'line', yAxisIndex: 1, smooth: true, data: r.perSecond.map(x => x.p99Ms), itemStyle: { color: SERIES[1] } }],
  });
  const d = card('Latency distribution', 'How many requests took how long (log scale). A tight hump is good; a long right tail means some requests waited much longer.');
  $('#load-c2').appendChild(d);
  const series = [];
  if (hasW) series.push({ name: 'writes', type: 'bar', data: r.writes.distribution.map(([x, n]) => [x, n]), itemStyle: { color: SERIES[0] }, barMaxWidth: 10 });
  if (hasR) series.push({ name: 'reads', type: 'bar', data: r.reads.distribution.map(([x, n]) => [x, n]), itemStyle: { color: SERIES[2] }, barMaxWidth: 10 });
  chart($('.chart', d), {
    tooltip: { trigger: 'item', formatter: p => `${p.seriesName}: ≤ ${ms(p.value[0])} ms<br><b>${fmt(p.value[1])}</b> requests` },
    xAxis: { type: 'log', name: 'latency (ms, log scale)', nameLocation: 'middle', logBase: 10 },
    yAxis: { type: 'value', name: 'requests' }, series,
  });
  const p = card('Percentiles', 'p99 = 99% of requests were faster than this. The tail (p99, p99.9) is what users notice.');
  $('#load-c2').appendChild(p);
  const P = ['p50Ms', 'p90Ms', 'p99Ms', 'p999Ms', 'maxMs'];
  chart($('.chart', p), {
    tooltip: { trigger: 'axis', valueFormatter: v => ms(v) + ' ms' }, legend: { data: [hasW && 'writes', hasR && 'reads'].filter(Boolean) },
    xAxis: { type: 'category', data: ['p50', 'p90', 'p99', 'p99.9', 'max'] },
    yAxis: { type: 'value', name: 'ms' },
    series: [hasW && { name: 'writes', type: 'bar', data: P.map(k => r.writes[k]), itemStyle: { color: SERIES[0], borderRadius: [4, 4, 0, 0] },
                       label: { show: true, position: 'top', color: c.muted, formatter: q => ms(q.value) } },
             hasR && { name: 'reads', type: 'bar', data: P.map(k => r.reads[k]), itemStyle: { color: SERIES[2], borderRadius: [4, 4, 0, 0] },
                       label: { show: true, position: 'top', color: c.muted, formatter: q => ms(q.value) } }].filter(Boolean),
  });
}

// ------------------------------------------------------------------ failover results

function renderFailoverLive(live) {
  const root = $('#fo-results');
  let el = $('#fo-live', root);
  if (!el && !live.length) return;   // keep "Starting…" until the first kill
  if (!el) {
    disposeIn(root);
    root.innerHTML = '';
    el = card('Live: each leader kill', 'Election = kill → a new leader exists. Client gap = the longest time the writing client got no answer.');
    el.id = 'fo-live';
    root.appendChild(el);
  }
  drawKills($('.chart', el), live);
}

function drawKills(el, kills) {
  chart(el, {
    tooltip: { trigger: 'axis', valueFormatter: v => ms(v) + ' ms' }, legend: { data: ['election', 'client gap'] },
    xAxis: { type: 'category', data: kills.map(k => '#' + k.n), name: 'kill', nameLocation: 'middle' },
    yAxis: { type: 'value', name: 'ms' },
    series: [{ name: 'election', type: 'bar', data: kills.map(k => k.electionMs), itemStyle: { color: SERIES[0], borderRadius: [3, 3, 0, 0] } },
      { name: 'client gap', type: 'bar', data: kills.map(k => k.clientGapMs), itemStyle: { color: SERIES[1], borderRadius: [3, 3, 0, 0] } }],
  });
}

function renderFailover(rec) {
  const root = $('#fo-results');
  disposeIn(root);
  const r = rec.result;
  root.innerHTML = `
    <div class="title-row"><h2>${esc(rec.label)}</h2><span class="meta">${when(rec.createdAt)} · commit ${esc(rec.gitCommit)}</span></div>
    <div class="kpis">
      ${kpi('Election p50', `${ms(r.election.p50Ms)}<small>ms</small>`, 'kill → new leader')}
      ${kpi('Election p99', `${ms(r.election.p99Ms)}<small>ms</small>`, `max ${ms(r.election.maxMs)} ms`)}
      ${kpi('Client gap p50', `${ms(r.clientGap.p50Ms)}<small>ms</small>`, 'longest wait for a write')}
      ${kpi('Client gap p99', `${ms(r.clientGap.p99Ms)}<small>ms</small>`, `max ${ms(r.clientGap.maxMs)} ms`)}
      ${kpi('Leader kills', fmt(r.kills.length), 'every one recovered', 'good')}
    </div>
    <div class="banner info">Why ~150–300 ms: followers wait one randomized election timeout (150–300 ms here) without heartbeats, then PreVote and a vote round (a few ms on localhost).</div>
    <div class="row2" id="fo-row"></div><div id="fo-kills"></div>`;
  const h = card('Distribution', 'How many kills took how long, in 25 ms buckets.');
  $('#fo-row').appendChild(h);
  const bucket = 25, max = Math.max(...r.kills.map(k => Math.max(k.electionMs, k.clientGapMs)), 1);
  const edges = []; for (let x = 0; x <= max + bucket; x += bucket) edges.push(x);
  const count = key => edges.slice(0, -1).map((e, i) => r.kills.filter(k => k[key] >= e && k[key] < edges[i + 1]).length);
  chart($('.chart', h), {
    tooltip: { trigger: 'axis' }, legend: { data: ['election', 'client gap'] },
    xAxis: { type: 'category', data: edges.slice(0, -1).map(e => `${e}–${e + bucket}`), name: 'ms', nameLocation: 'middle' },
    yAxis: { type: 'value', name: 'kills', minInterval: 1 },
    series: [{ name: 'election', type: 'bar', data: count('electionMs'), itemStyle: { color: SERIES[0] } },
      { name: 'client gap', type: 'bar', data: count('clientGapMs'), itemStyle: { color: SERIES[1] } }],
  });
  const b = card('Spread', 'Box = middle half of the kills, line = median, whiskers = min and max.');
  $('#fo-row').appendChild(b);
  const box = key => { const v = r.kills.map(k => k[key]).sort((x, y) => x - y); const q = f => v[Math.min(v.length - 1, Math.floor(f * (v.length - 1)))]; return [v[0], q(.25), q(.5), q(.75), v[v.length - 1]]; };
  chart($('.chart', b), {
    tooltip: { trigger: 'item', formatter: p => `${p.name}<br>min ${ms(p.value[1])} · q1 ${ms(p.value[2])} · median ${ms(p.value[3])} · q3 ${ms(p.value[4])} · max ${ms(p.value[5])} ms` },
    xAxis: { type: 'category', data: ['election', 'client gap'] }, yAxis: { type: 'value', name: 'ms' },
    series: [{ type: 'boxplot', data: [box('electionMs'), box('clientGapMs')], itemStyle: { color: C().panel, borderColor: SERIES[0], borderWidth: 2 } }],
  });
  const k = card('Every kill', 'Hover a bar: which node died, which took over, in which term.');
  $('#fo-kills').appendChild(k);
  drawKills($('.chart', k), r.kills);
  charts.get($('.chart', k)).setOption({ tooltip: { trigger: 'axis', formatter: ps => {
    const x = r.kills[ps[0].dataIndex];
    return `kill #${x.n}: n${x.oldLeader} → n${x.newLeader} (term ${x.newTerm})<br>election ${ms(x.electionMs)} ms · client gap ${ms(x.clientGapMs)} ms`;
  } } });
}

// ------------------------------------------------------------------ compare

let cmpKind = 'load';
const cmpSelected = new Set();
$('#cmp-kind').addEventListener('change', () => { cmpKind = $('#cmp-kind button.on').value; cmpSelected.clear(); loadCompareList(); });
$('#cmp-all').onclick = () => { runs.filter(r => r.kind === cmpKind).forEach(r => cmpSelected.add(r.id)); loadCompareList(); };
$('#cmp-none').onclick = () => { cmpSelected.clear(); loadCompareList(); };

function loadCompareList() {
  const mine = runs.filter(r => r.kind === cmpKind);
  $('#cmp-list').innerHTML = mine.length ? mine.map(r => `<li data-id="${r.id}" class="${cmpSelected.has(r.id) ? 'sel' : ''}">
      <input type="checkbox" ${cmpSelected.has(r.id) ? 'checked' : ''}><div class="rl-main"><div class="rl-title">${esc(r.label)}</div>
      <div class="rl-meta">${when(r.createdAt)} · ${headline(r)}</div></div></li>`).join('') : '<li class="muted">no saved runs of this kind</li>';
  $$('#cmp-list li[data-id]').forEach(li => li.onclick = () => {
    cmpSelected.has(li.dataset.id) ? cmpSelected.delete(li.dataset.id) : cmpSelected.add(li.dataset.id);
    loadCompareList();
  });
  renderCompare(mine.filter(r => cmpSelected.has(r.id)));
}

function renderCompare(sel) {
  const root = $('#cmp-results');
  disposeIn(root);
  if (sel.length < 1) { root.innerHTML = '<div class="empty">Tick two or more runs on the left.</div>'; return; }
  root.innerHTML = '';
  if (cmpKind === 'load') compareLoad(root, sel);
  if (cmpKind === 'failover') compareFailover(root, sel);
  if (cmpKind === 'sim') compareSim(root, sel);
}

function compareLoad(root, sel) {
  // Series = (nodes, batching, write %); x = clients; y = median over repeated runs.
  const groups = new Map();
  for (const r of sel) {
    const s = r.settings;
    // Where it ran is part of the series: a Docker run must never be averaged with a local-JVM run.
    const env = (r.label.match(/\(([^)]+)\)\s*$/) || [])[1];
    const key = `${s.nodes} nodes · batching ${s.fsyncBatching ? 'on' : 'off'}${s.writePercent === 100 ? '' : ` · ${s.writePercent}% writes`}${env ? ` · ${env}` : ''}`;
    if (!groups.has(key)) groups.set(key, new Map());
    const byClients = groups.get(key);
    if (!byClients.has(s.clients)) byClients.set(s.clients, []);
    byClients.get(s.clients).push(r.headline);
  }
  const clients = [...new Set(sel.map(r => r.settings.clients))].sort((a, b) => a - b);
  const keys = [...groups.keys()].sort();
  const line = (title, explain, pickFn, unit) => {
    const el = card(title, explain);
    root.appendChild(el);
    chart($('.chart', el), {
      tooltip: { trigger: 'axis', valueFormatter: v => v == null ? '–' : `${fmt(v, unit === 'ms' ? 2 : 0)} ${unit}` },
      legend: { data: keys, type: 'scroll' },
      grid: { left: 64, right: 24, top: 48, bottom: 44 },
      xAxis: { type: 'category', data: clients, name: 'concurrent clients', nameLocation: 'middle' },
      yAxis: { type: 'value', name: unit },
      series: keys.map((k, i) => ({ name: k, type: 'line', symbolSize: 8, smooth: false, connectNulls: true,
        lineStyle: { width: 2.5, type: k.includes('batching off') ? 'dashed' : 'solid' }, itemStyle: { color: SERIES[i % SERIES.length] },
        // Point labels only while they can't pile up; the tooltip has every value.
        label: { show: keys.length <= 3, position: 'top', fontSize: 10, color: C().muted, formatter: p => fmt(p.value, unit === 'ms' ? 1 : 0) },
        data: clients.map(cl => { const hs = groups.get(k).get(cl); return hs ? median(hs.map(pickFn)) : null; }) })),
    });
  };
  if (clients.length > 1) {
    line('Throughput as clients increase', 'Median of repeated runs. More clients → more requests waiting → bigger batches per fsync (when group commit is on). Dashed = batching off.', h => h.opsPerSec, 'ops/s');
    line('Tail latency (p99) as clients increase', 'Write p99 (read p99 for read-only runs). Lower is better.', h => h.writeP99 || h.readP99, 'ms');
  }
  const bar = card('Run by run', 'Each ticked run: throughput (bars) and p99 latency (dots).');
  bar.querySelector('.chart').classList.add('tall');
  root.appendChild(bar);
  chart($('.chart', bar), {
    tooltip: { trigger: 'axis' }, legend: { data: ['ops/s', 'p99 ms'] },
    grid: { left: 64, right: 64, top: 36, bottom: 110 },
    xAxis: { type: 'category', data: sel.map(r => r.label), axisLabel: { rotate: 30, fontSize: 10, interval: sel.length <= 16 ? 0 : 'auto', width: 160, overflow: 'truncate' } },
    yAxis: [{ type: 'value', name: 'ops/s' }, { type: 'value', name: 'p99 ms', splitLine: { show: false } }],
    series: [{ name: 'ops/s', type: 'bar', data: sel.map(r => r.headline.opsPerSec), itemStyle: { color: SERIES[0], borderRadius: [4, 4, 0, 0] },
               label: { show: sel.length <= 16, position: 'top', fontSize: 10, color: C().muted, formatter: p => fmt(p.value) } },
             { name: 'p99 ms', type: 'scatter', yAxisIndex: 1, symbolSize: 10, data: sel.map(r => r.headline.writeP99 || r.headline.readP99), itemStyle: { color: SERIES[1] } }],
  });
  root.insertAdjacentHTML('beforeend', `<div class="card"><h3>Numbers</h3><div class="tablewrap"><table class="data"><thead><tr><th>Run</th><th>ops/s</th><th>write p50</th><th>write p99</th><th>read p50</th><th>read p99</th><th>fsync mean</th><th>events / fsync</th></tr></thead>
    <tbody>${sel.map(r => { const h = r.headline; const m = v => v ? ms(v) : '–'; return `<tr><td>${esc(r.label)}</td><td>${fmt(h.opsPerSec)}</td><td>${m(h.writeP50)}</td><td>${m(h.writeP99)}</td><td>${m(h.readP50)}</td><td>${m(h.readP99)}</td><td>${ms(h.fsyncMeanMs)}</td><td>${fmt(h.batchMean, 1)}</td></tr>`; }).join('')}</tbody></table></div></div>`);
}

function compareFailover(root, sel) {
  const el = card('Failover time per run', 'p50 and p99 of election time and client gap for each ticked run.');
  root.appendChild(el);
  const names = sel.map(r => r.label);
  chart($('.chart', el), {
    tooltip: { trigger: 'axis', valueFormatter: v => ms(v) + ' ms' }, legend: { data: ['election p50', 'election p99', 'client gap p50', 'client gap p99'] },
    grid: { left: 56, right: 24, top: 48, bottom: 90 },
    xAxis: { type: 'category', data: names, axisLabel: { rotate: 20, fontSize: 10, interval: 0 } }, yAxis: { type: 'value', name: 'ms' },
    series: [['election p50', 'electionP50', 0], ['election p99', 'electionP99', 0], ['client gap p50', 'gapP50', 1], ['client gap p99', 'gapP99', 1]]
      .map(([n, k, ci], i) => ({ name: n, type: 'bar', data: sel.map(r => r.headline[k]), itemStyle: { color: SERIES[ci], opacity: i % 2 ? 1 : .55 } })),
  });
}

function compareSim(root, sel) {
  const cats = [...new Set(sel.flatMap(r => Object.keys(r.headline.outcomes || {})))].sort((a, b) => (a === 'passed') - (b === 'passed'));
  const el = card('Outcomes per run', 'Stacked: seeds passed (green) and seeds caught by each checker.');
  root.appendChild(el);
  const c = C();
  chart($('.chart', el), {
    tooltip: { trigger: 'axis' }, legend: { data: cats },
    grid: { left: 56, right: 24, top: 48, bottom: 90 },
    xAxis: { type: 'category', data: sel.map(r => r.label), axisLabel: { rotate: 20, fontSize: 10, interval: 0, width: 200, overflow: 'truncate' } },
    yAxis: { type: 'value', name: 'seeds' },
    series: cats.map((k, i) => ({ name: k, type: 'bar', stack: 'all', data: sel.map(r => (r.headline.outcomes || {})[k] || 0),
      itemStyle: { color: k === 'passed' ? c.pass : [c.fail, '#c2255c', c.warn, '#7048e8', '#1098ad'][i % 5] } })),
  });
}

init();
