/* Raft KV Lab: the "Benchmarks" tab. A fixed, explained report of the M6 measurements (not a live run).
 * Every number below is a median of saved runs in results/ (load-*.json, failover-*.json), measured
 * 2026-11-27 unless noted. Uses the helpers from app.js (chart, card, kpi, SERIES, C, fmt, ms, $). */
'use strict';

const BENCH = {
  // Local JVMs, 15 s measured after 3 s warm-up, median of 3 repeats.
  // key: `${nodes}-${batching ? 'on' : 'off'}-${writePercent}` → clients → [ops/s, write p50, write p99, read p50, read p99, fsync mean ms, writes per fsync]
  matrix: {
    '3-off-100': { 1: [124, 8.9, 10.7], 4: [143, 29.9, 44.7], 16: [158, 93.0, 192.0], 64: [148, 442.4, 540.7] },
    '3-on-100':  { 1: [149, 6.7, 10.3, 0, 0, 2.99, 1.2], 4: [167, 24.0, 36.2, 0, 0, 5.56, 3.6], 16: [574, 28.1, 41.5, 0, 0, 5.13, 12.7], 64: [2075, 30.9, 49.5, 0, 0, 4.64, 45.0] },
    '3-off-50':  { 1: [268, 6.9, 10.0, 0.4, 1.6], 4: [257, 17.7, 39.0, 11.0, 27.1], 16: [287, 46.1, 107.7, 60.7, 111.6], 64: [285, 161.5, 342.0, 278.0, 388.9] },
    '3-on-50':   { 1: [238, 8.0, 11.4, 0.4, 1.8], 4: [270, 17.9, 32.8, 11.8, 27.0], 16: [604, 25.9, 39.0, 27.3, 41.2], 64: [2017, 30.4, 44.8, 32.6, 51.1] },
    '5-off-100': { 1: [103, 9.5, 15.0], 4: [123, 32.9, 46.0], 16: [135, 113.0, 188.3], 64: [119, 534.5, 764.4] },
    '5-on-100':  { 1: [111, 8.9, 15.0], 4: [142, 28.0, 42.2], 16: [392, 40.8, 60.4], 64: [1234, 51.9, 80.0] },
    '5-off-50':  { 1: [202, 9.1, 14.9, 0.5, 1.8], 4: [229, 20.7, 39.0, 13.6, 30.0], 16: [258, 51.7, 117.2, 68.6, 124.0], 64: [248, 193.3, 403.5, 309.2, 457.0] },
    '5-on-50':   { 1: [210, 8.7, 15.0, 0.5, 2.2], 4: [238, 19.9, 36.0, 13.0, 30.2], 16: [500, 31.4, 49.0, 33.0, 54.9], 64: [1312, 47.0, 69.3, 50.5, 77.6] },
  },
  // 100 leader kills each (kill -9), group commit on. [p50, p99, max] in ms.
  failover: {
    3: { election: [199.0, 385.8, 409.1], gap: [211.7, 399.4, 410.4], splitVotes: 2 },
    5: { election: [185.6, 247.4, 250.2], gap: [204.3, 268.8, 271.4], splitVotes: 0 },
  },
  // docker compose, 3 nodes, group commit on, writes only, median of 3. [ops/s, write p50, write p99]
  docker: {
    before: { 1: [20, 51.1, 55.6], 16: [363, 44.0, 49.2], 64: [1396, 45.9, 52.4] },
    after:  { 1: [543, 1.6, 4.2], 16: [2940, 4.9, 12.0], 64: [4655, 13.1, 24.0] },
  },
  // One 4 KB write + flush, same APFS volume, median and p99 in ms.
  fsync: [['fsync()', 0.03, null], ['fcntl(F_FULLFSYNC)', 2.97, 4.08], ['Java force(true)', 2.96, 4.06]],
};

const CLIENTS = [1, 4, 16, 64];
const col = (key, i) => CLIENTS.map(c => BENCH.matrix[key][c][i]);

function benchCard(title, explain, why, tall) {
  const el = card(title, explain);
  if (tall) $('.chart', el).classList.add('tall');
  if (why) el.insertAdjacentHTML('beforeend', `<div class="why">${why}</div>`);
  return el;
}

function lineOpt(unit, series, extra = {}) {
  return {
    tooltip: { trigger: 'axis', valueFormatter: v => v == null ? '–' : `${fmt(v, unit === 'ms' ? 1 : 0)} ${unit}` },
    legend: { data: series.map(s => s.name) },
    grid: { left: 64, right: 24, top: 44, bottom: 44 },
    xAxis: { type: 'category', data: CLIENTS, name: 'concurrent clients', nameLocation: 'middle' },
    yAxis: { type: 'value', name: unit },
    series: series.map((s, i) => ({ type: 'line', symbolSize: 9, lineStyle: { width: 3, type: s.dashed ? 'dashed' : 'solid' },
      itemStyle: { color: s.color || SERIES[i] }, label: { show: true, position: 'top', fontSize: 11, color: C().muted,
      formatter: p => fmt(p.value, unit === 'ms' && p.value < 100 ? 1 : 0) }, ...s })),
    ...extra,
  };
}

let benchDrawn = false;
function renderBenchmarks() {
  const root = $('#bench-results');
  if (benchDrawn) return;
  benchDrawn = true;
  const c = C();
  const on = BENCH.matrix['3-on-100'], off = BENCH.matrix['3-off-100'];

  root.innerHTML = `
    <div class="bench-hero">
      <h2>What the cluster can do, measured</h2>
      <p>Real <code>raft-server</code> processes, real disk flushes, real <code>kill -9</code>. Each number is the median of 3 runs
         (100 kills for failover). The cards below explain what each chart shows, why it looks that way, and what to take away.</p>
    </div>
    <div class="kpis">
      ${kpi('Group commit speed-up', `${fmt(on[64][0] / off[64][0], 0)}<small>×</small>`, `${fmt(off[64][0])} → ${fmt(on[64][0])} writes/s · 3 nodes, 64 clients`, 'good')}
      ${kpi('Write p99 at 64 clients', `${fmt(on[64][2], 0)}<small>ms</small>`, `was ${fmt(off[64][2], 0)} ms without group commit`, 'good')}
      ${kpi('Leader failover p50', `${fmt(BENCH.failover[3].election[0], 0)}<small>ms</small>`, `p99 ${fmt(BENCH.failover[3].election[1], 0)} ms · 100 kills, 3 nodes`)}
      ${kpi('Every write durable', '<small>fsync</small> ✓', 'acked only after a real flush to the SSD on a majority', 'good')}
      ${kpi('Timeouts', '0', 'in 96 matrix runs (about 30 min of load)', 'good')}
      ${kpi('Linux (Docker)', `${fmt(BENCH.docker.after[64][0])}<small>ops/s</small>`, '3 containers, 64 clients', '')}
    </div>
    <div class="card how">
      <h3>How it was measured</h3>
      <div class="how-grid">
        <div><b>Machine</b><span>One MacBook (Apple silicon, 11 CPUs). All 3 or 5 nodes run on it as separate JVMs and share its CPU and SSD.</span></div>
        <div><b>Clients</b><span>Closed loop: each client sends one request, waits for the answer, then sends the next. More clients = more load.</span></div>
        <div><b>Workload</b><span>1,000 keys, 100-byte values. "Writes only" = every request is a PUT; "50% writes" = half PUT, half linearizable GET.</span></div>
        <div><b>Timing</b><span>3 s warm-up (not counted), then 15 s measured. Each setting ran 3 times; the median is shown. Most repeats agree within 15%.</span></div>
        <div><b>Durability</b><span>A write is answered only after the leader and a majority have flushed it to disk (<code>F_FULLFSYNC</code> on macOS, about 3 ms).</span></div>
        <div><b>Group commit</b><span>Off: one disk flush per request. On: the server drains everything waiting in its mailbox and flushes once for the whole batch.</span></div>
      </div>
    </div>`;

  // 1. Throughput
  const t = benchCard('1 · Throughput: group commit is the big win',
    '3 nodes, writes only. Writes per second as the number of clients grows. Solid = group commit on, dashed = off.',
    `<b>Why:</b> without group commit every write waits for its own disk flush (3–6 ms), one after another, so the cluster is stuck near
     150 writes/s no matter how many clients send. With group commit, while one flush runs the next requests pile up, and the next flush
     saves all of them at once: at 64 clients one flush carries about 45 writes. <b>Takeaway:</b> the disk flush, not Raft's messages, is the bottleneck,
     and batching turns one cost per write into one cost per batch.`);
  root.appendChild(t);
  chart($('.chart', t), lineOpt('ops/s', [
    { name: 'group commit on', data: col('3-on-100', 0), color: c.pass },
    { name: 'group commit off', data: col('3-off-100', 0), color: c.fail, dashed: true },
  ]));

  // 2. Latency
  const l = benchCard('2 · Tail latency: what the slowest 1% of writes wait',
    '3 nodes, writes only. p99 write latency (99% of writes were faster than this).',
    `<b>Why:</b> with flushes one at a time, a write must wait for every write ahead of it in the queue. Little's law checks it:
     64 clients ÷ 148 writes/s ≈ 432 ms of waiting, and the measured p50 is 442 ms. With group commit: 64 ÷ 2,075 ≈ 31 ms, measured 31 ms.
     <b>Takeaway:</b> group commit improves both throughput <i>and</i> latency at the same time, because the waiting was the latency.`);
  root.appendChild(l);
  chart($('.chart', l), lineOpt('ms', [
    { name: 'group commit on', data: col('3-on-100', 2), color: c.pass },
    { name: 'group commit off', data: col('3-off-100', 2), color: c.fail, dashed: true },
  ]));

  // 3. Batch size
  const b = benchCard('3 · How group commit works: writes saved per disk flush',
    '3 nodes, writes only, group commit on. Average number of writes in one flush on the leader.',
    `<b>Why:</b> batching adapts by itself. With 1 client there is nothing to batch (about 1 write per flush, same as off). As more clients wait, each
     flush picks up everyone who arrived during the previous one. No timer is involved, so a lone client never waits for a batch to fill.
     <b>Takeaway:</b> the batch grows with the load, which is why the throughput curve bends upward.`);
  root.appendChild(b);
  chart($('.chart', b), {
    tooltip: { trigger: 'axis', valueFormatter: v => `${fmt(v, 1)} writes per flush` },
    grid: { left: 64, right: 24, top: 30, bottom: 44 },
    xAxis: { type: 'category', data: CLIENTS, name: 'concurrent clients', nameLocation: 'middle' },
    yAxis: { type: 'value', name: 'writes / flush' },
    series: [{ type: 'bar', data: col('3-on-100', 6), barWidth: '45%', itemStyle: { color: c.accent, borderRadius: [6, 6, 0, 0] },
      label: { show: true, position: 'top', color: c.text, fontWeight: 700, formatter: p => fmt(p.value, 1) } }],
  });

  const row = document.createElement('div');
  row.className = 'bench-row';
  root.appendChild(row);

  // 4. 3 vs 5 nodes
  const n = benchCard('4 · 3 nodes vs 5 nodes',
    '64 clients, group commit on. Writes per second.',
    `<b>Why:</b> with 5 nodes the leader sends every entry to 4 followers instead of 2 and waits for 3 flushes instead of 2. Here all 5 JVMs
     also share one laptop's CPU and SSD, so part of the drop is the laptop. <b>Takeaway:</b> more nodes buy fault tolerance
     (5 nodes survive 2 failures, 3 nodes survive 1), not speed.`);
  row.appendChild(n);
  chart($('.chart', n), {
    tooltip: { trigger: 'axis', valueFormatter: v => `${fmt(v)} ops/s` },
    legend: { data: ['3 nodes', '5 nodes'] },
    grid: { left: 64, right: 16, top: 44, bottom: 30 },
    xAxis: { type: 'category', data: ['writes only', '50% writes'] },
    yAxis: { type: 'value', name: 'ops/s' },
    series: [['3 nodes', 3, SERIES[0]], ['5 nodes', 5, SERIES[1]]].map(([name, k, color]) => ({ name, type: 'bar', itemStyle: { color, borderRadius: [6, 6, 0, 0] },
      data: [BENCH.matrix[`${k}-on-100`][64][0], BENCH.matrix[`${k}-on-50`][64][0]],
      label: { show: true, position: 'top', color: c.text, formatter: p => fmt(p.value) } })),
  });

  // 5. Reads vs writes
  const rw = benchCard('5 · Reads vs writes',
    '3 nodes, 50% writes, group commit on. Median latency.',
    `<b>Why:</b> a read does not go into the log and needs no disk flush. The leader only confirms with one heartbeat round that it is still the
     leader (ReadIndex), so an idle read takes 0.4 ms against 8 ms for a write. Under load, reads wait about as long as writes; my guess is that
     they queue behind writes on the server's single event loop, but that is not measured yet.
     <b>Takeaway:</b> reads stay linearizable without paying for a log entry.`);
  row.appendChild(rw);
  const m50 = BENCH.matrix['3-on-50'];
  chart($('.chart', rw), {
    tooltip: { trigger: 'axis', valueFormatter: v => `${ms(v)} ms` },
    legend: { data: ['write p50', 'read p50'] },
    grid: { left: 56, right: 16, top: 44, bottom: 30 },
    xAxis: { type: 'category', data: ['1 client (idle)', '64 clients (busy)'] },
    yAxis: { type: 'value', name: 'ms' },
    series: [['write p50', 1, SERIES[0]], ['read p50', 3, SERIES[2]]].map(([name, i, color]) => ({ name, type: 'bar', itemStyle: { color, borderRadius: [6, 6, 0, 0] },
      data: [m50[1][i], m50[64][i]], label: { show: true, position: 'top', color: c.text, formatter: p => ms(p.value) } })),
  });

  // 6. Failover
  const f = benchCard('6 · Leader failover: how long the cluster has no leader',
    'The leader is killed with kill -9 while a client keeps writing; 100 kills per cluster size. Election = kill → new leader; client gap = the client\'s longest wait for a write.',
    `<b>Why:</b> followers notice the leader is gone only when no heartbeat arrives for a random election timeout of 150–300 ms, so the median
     lands near the middle, about 200 ms. The vote itself takes a few ms on localhost. The client gap is about 10–20 ms longer because the
     client must also find the new leader. The 3-node p99 comes from 2 kills (of 100) where two followers timed out together, split the vote and
     needed a second round; the 5-node run had none. <b>Takeaway:</b> every kill recovered, with no lost acknowledged writes, in well under half a second.`, true);
  root.appendChild(f);
  const fo = BENCH.failover;
  chart($('.chart', f), {
    tooltip: { trigger: 'axis', valueFormatter: v => `${ms(v)} ms` },
    legend: { data: ['p50', 'p99', 'max'] },
    grid: { left: 56, right: 16, top: 44, bottom: 30 },
    xAxis: { type: 'category', data: ['3 nodes · election', '3 nodes · client gap', '5 nodes · election', '5 nodes · client gap'] },
    yAxis: { type: 'value', name: 'ms' },
    series: ['p50', 'p99', 'max'].map((name, i) => ({ name, type: 'bar', itemStyle: { color: [c.pass, c.warn, c.fail][i], borderRadius: [5, 5, 0, 0] },
      data: [fo[3].election[i], fo[3].gap[i], fo[5].election[i], fo[5].gap[i]],
      label: { show: true, position: 'top', fontSize: 11, color: c.text, formatter: p => fmt(p.value, 0) } })),
  });

  const row2 = document.createElement('div');
  row2.className = 'bench-row';
  root.appendChild(row2);

  // 7. Docker + Nagle
  const d = benchCard('7 · On Linux (Docker): the 40 ms bug benchmarks found',
    '3 containers, group commit on, writes only. Writes per second before and after the fix, next to the Mac numbers.',
    `<b>What happened:</b> on Docker one client got only 20 writes/s, each taking about 51 ms. The server answered every request in two pieces
     (headers, then body). TCP's Nagle algorithm held back the second piece until the first was acknowledged, and Linux delays that
     acknowledgement by up to 40 ms. macOS did not show it. <b>Fix:</b> turn on <code>TCP_NODELAY</code>: 20 → 543 writes/s for one client.
     <b>Note:</b> Docker is faster than the Mac because its Linux VM flushes in about 0.5–0.9 ms instead of 3 ms. Whether that flush is as durable
     as macOS <code>F_FULLFSYNC</code> is not verified, so the Mac numbers are the ones to trust.`, true);
  row2.appendChild(d);
  const dk = BENCH.docker;
  chart($('.chart', d), {
    tooltip: { trigger: 'axis', valueFormatter: v => `${fmt(v)} ops/s` },
    legend: { data: ['Docker, before fix', 'Docker, after fix', 'Mac (local JVMs)'] },
    grid: { left: 64, right: 16, top: 44, bottom: 44 },
    xAxis: { type: 'category', data: ['1 client', '16 clients', '64 clients'], name: '', nameLocation: 'middle' },
    yAxis: { type: 'value', name: 'ops/s' },
    series: [['Docker, before fix', [dk.before[1][0], dk.before[16][0], dk.before[64][0]], c.fail],
             ['Docker, after fix', [dk.after[1][0], dk.after[16][0], dk.after[64][0]], c.pass],
             ['Mac (local JVMs)', [on[1][0], on[16][0], on[64][0]], SERIES[0]]].map(([name, data, color]) => ({ name, type: 'bar', data,
      itemStyle: { color, borderRadius: [5, 5, 0, 0] }, label: { show: true, position: 'top', fontSize: 11, color: c.text, formatter: p => fmt(p.value) } })),
  });

  // 8. fsync on macOS
  const s = benchCard('8 · Is the disk flush real? (macOS)',
    'Median time to write 4 KB and flush it, three ways, on the same SSD.',
    `<b>Why it matters:</b> on macOS plain <code>fsync()</code> only hands data to the drive's cache (0.03 ms) and can be lost on power failure.
     Only <code>F_FULLFSYNC</code> forces it onto the flash (about 3 ms). Java's <code>force(true)</code> takes the same 3 ms, so on this JDK it does the
     full flush. <b>Takeaway:</b> every number on this page is a truly durable number, not a fast-but-unsafe one.`);
  row2.appendChild(s);
  chart($('.chart', s), {
    tooltip: { trigger: 'axis', valueFormatter: v => v == null ? '–' : `${ms(v)} ms` },
    grid: { left: 140, right: 50, top: 20, bottom: 30 },
    xAxis: { type: 'value', name: 'ms' },
    yAxis: { type: 'category', data: BENCH.fsync.map(x => x[0]), inverse: true },
    series: [{ type: 'bar', data: BENCH.fsync.map((x, i) => ({ value: x[1], itemStyle: { color: i === 0 ? c.warn : c.pass, borderRadius: [0, 6, 6, 0] } })),
      barWidth: '50%', label: { show: true, position: 'right', color: c.text, fontWeight: 700, formatter: p => `${ms(p.value)} ms` } }],
  });

  // 9. Caveats + full table
  root.insertAdjacentHTML('beforeend', `
    <div class="card how">
      <h3>Honest limits of these numbers</h3>
      <ul class="limits">
        <li><b>One laptop.</b> All nodes share one CPU and one SSD, and messages travel over localhost (no real network delay). On separate machines
            each node gets its own disk, but every write also pays a network round trip.</li>
        <li><b>Shared machine.</b> Other apps (IDE, browser) were open. Repeats mostly agree within 15%; the worst setting varied by 39% (3 nodes, group commit off, 16 clients).</li>
        <li><b>Small values, simple workload.</b> 100-byte values, uniformly random keys, no large snapshots during the runs.</li>
        <li><b>Docker durability</b> is not verified (see card 7), so Docker numbers are for comparison only.</li>
      </ul>
    </div>
    <div class="card">
      <div class="card-head"><div><h3>Every measured setting</h3><p class="explain">Local JVMs, median of 3 runs. p50 = typical request, p99 = slowest 1%. "–" = no reads in that workload.</p></div></div>
      <div class="tablewrap"><table class="data bench-table"><thead><tr><th>Nodes</th><th>Group commit</th><th>Workload</th><th>Clients</th>
        <th>ops/s</th><th>write p50</th><th>write p99</th><th>read p50</th><th>read p99</th></tr></thead><tbody>
        ${Object.entries(BENCH.matrix).map(([key, byC]) => { const [nodes, bat, w] = key.split('-'); return CLIENTS.map(cl => { const v = byC[cl];
          const m = x => x ? ms(x) : '–';
          return `<tr><td>${nodes}</td><td><span class="tag ${bat}">${bat}</span></td><td>${w === '100' ? 'writes only' : '50% writes'}</td><td>${cl}</td>
            <td><b>${fmt(v[0])}</b></td><td>${m(v[1])}</td><td>${m(v[2])}</td><td>${m(v[3])}</td><td>${m(v[4])}</td></tr>`; }).join(''); }).join('')}
      </tbody></table></div>
    </div>`);
}

$('.tabs button[data-tab="bench"]').addEventListener('click', () => setTimeout(renderBenchmarks, 0));
