/**
 * 짧은 봉(1분·5분) 최근 구간 수집 — 확정 봉만, 시각순, 중복 제거. 빠진 봉은 채우지 않고 개수만 기록
 * 실행: node scripts/data/fetch-okx-lowtf.mjs <outDir> <COIN> <bar 1m|5m> <days>
 */
import fs from "node:fs";
import path from "node:path";
import zlib from "node:zlib";
const [OUT, COIN, BAR, DAYS] = process.argv.slice(2);
const inst = `${COIN}-USDT-SWAP`;
const MS = { "1m": 60_000, "5m": 300_000 }[BAR];
if (!MS) throw new Error("bar 1m|5m");
const start = Date.now() - Number(DAYS) * 86_400_000;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function get(qs) {
  for (let i = 0; i < 8; i++) {
    try {
      const r = await fetch(`https://www.okx.com/api/v5/market/history-candles?${qs}`);
      const j = await r.json();
      if (j.code === "0") return j.data;
      await sleep(1500 * (i + 1));
    } catch {
      await sleep(1500 * (i + 1));
    }
  }
  throw new Error("fetch failed " + qs);
}
const got = new Map();
let after = Date.now();
for (;;) {
  const d = await get(`instId=${inst}&bar=${BAR}&after=${after}&limit=100`);
  await sleep(110);
  if (!d?.length) break;
  for (const x of d) if (x[8] === "1") got.set(+x[0], [+x[0], +x[1], +x[2], +x[3], +x[4], +x[5]]);
  const oldest = +d.at(-1)[0];
  if (oldest <= start) break;
  after = oldest;
}
const rows = [...got.values()].filter((r) => r[0] >= start).sort((a, b) => a[0] - b[0]);
let missing = 0;
for (let i = 1; i < rows.length; i++) missing += Math.max(0, (rows[i][0] - rows[i - 1][0]) / MS - 1);
fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(path.join(OUT, `${inst}_${BAR}.csv.gz`), zlib.gzipSync("ts,o,h,l,c,vol\n" + rows.map((r) => r.join(",")).join("\n") + "\n", { level: 9 }));
console.log(JSON.stringify({ inst, bar: BAR, rows: rows.length, first: rows[0]?.[0], last: rows.at(-1)?.[0], missing }));
