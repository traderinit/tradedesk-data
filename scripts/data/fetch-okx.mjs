/**
 * 1단계 데이터 엔진 — OKX 무기한 선물 캔들·펀딩비를 모아 data/okx 에 저장하고 검증한다.
 *  - 확정된 봉만 (confirm=1), 시각순, 중복 제거
 *  - 이미 받은 파일이 있으면 그 뒤만 이어 받는다
 *  - 검증: 빠진 봉 · 중복 · 0/음수 가격 · 고가<저가 · 한 봉 ±25% 이상 급변 → manifest.json 에 기록
 * 실행: node scripts/data/fetch-okx.mjs [outDir]   (GitHub Actions 에서 매일)
 */
import fs from "node:fs";
import path from "node:path";
import zlib from "node:zlib";

const OUT = process.argv[2] ?? "data/okx";
/** 개발 10코인 + 1차 평가 10코인 (src/lib/quant/universe.json). 15분은 앞 5코인만 */
const ALL = ["BTC-USDT-SWAP",  "ETH-USDT-SWAP",  "SOL-USDT-SWAP",  "XRP-USDT-SWAP",  "DOGE-USDT-SWAP",  "ADA-USDT-SWAP",  "AVAX-USDT-SWAP",  "LINK-USDT-SWAP",  "LTC-USDT-SWAP",  "BCH-USDT-SWAP",  "NEAR-USDT-SWAP",  "UNI-USDT-SWAP",  "TRX-USDT-SWAP",  "FIL-USDT-SWAP",  "AAVE-USDT-SWAP",  "DOT-USDT-SWAP",  "CRV-USDT-SWAP",  "XLM-USDT-SWAP",  "ETC-USDT-SWAP",  "RSR-USDT-SWAP"];
const INTRADAY = new Set(ALL.slice(0, 5));
/** 코인 하나만 받을 때 (GitHub Actions 에서 코인별로 나눠 동시에) */
const INSTS = process.argv[3] ? ALL.filter((x) => x.startsWith(process.argv[3] + "-")) : ALL;
const BARS = { "15m": 900_000, "1H": 3_600_000, "4H": 14_400_000, "1Dutc": 86_400_000 };
/** 15분은 2022년부터, 1시간·4시간·일봉은 상장 시점(최대 2019년)부터 */
const START_OF = { "15m": Date.UTC(2022, 0, 1), "1H": Date.UTC(2019, 0, 1), "4H": Date.UTC(2019, 0, 1), "1Dutc": Date.UTC(2019, 0, 1) };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function get(p, qs) {
  for (let i = 0; i < 6; i++) {
    try {
      const r = await fetch(`https://www.okx.com${p}?${qs}`, { headers: { accept: "application/json" } });
      const j = await r.json();
      if (j.code === "0") return j.data;
      if (j.code === "50011") { await sleep(2000); continue; } // 요청 과다
      throw new Error(`${p} ${j.code} ${j.msg}`);
    } catch (e) {
      if (i === 5) throw e;
      await sleep(1000 * (i + 1));
    }
  }
}

const readCsv = (f) => {
  if (!fs.existsSync(f)) return [];
  return zlib.gunzipSync(fs.readFileSync(f)).toString().trim().split("\n").slice(1).map((l) => l.split(",").map(Number));
};
const writeCsv = (f, head, rows) => fs.writeFileSync(f, zlib.gzipSync(head + "\n" + rows.map((r) => r.join(",")).join("\n") + "\n", { level: 9 }));

async function candles(inst, bar, ms) {
  const START = START_OF[bar];
  const f = path.join(OUT, `${inst}_${bar}.csv.gz`);
  const have = readCsv(f);
  // 저장본이 시작 시점보다 늦게 시작하면(예전 설정·늦게 상장) 처음부터 다시 받는다 (4시간·일봉은 요청 수가 적다)
  const from = have.length && have[0][0] <= START + ms * 2 ? have.at(-1)[0] : START - ms;
  const got = new Map(have.map((r) => [r[0], r]));
  let after = Date.now();
  for (;;) {
    const d = await get("/api/v5/market/history-candles", `instId=${inst}&bar=${bar}&after=${after}&limit=100`);
    await sleep(120);
    if (!d?.length) break;
    for (const x of d) if (x[8] === "1") got.set(+x[0], [+x[0], +x[1], +x[2], +x[3], +x[4], +x[5]]);
    const oldest = +d.at(-1)[0];
    if (oldest <= from || oldest <= START) break;
    after = oldest;
  }
  const rows = [...got.values()].filter((r) => r[0] >= START).sort((a, b) => a[0] - b[0]);
  writeCsv(f, "ts,o,h,l,c,vol", rows);
  // 검증
  let gaps = 0, missing = 0, bad = 0, jumps = 0;
  for (let i = 0; i < rows.length; i++) {
    const [t, o, h, l, c] = rows[i];
    if (!(o > 0 && h > 0 && l > 0 && c > 0) || h < l || h < Math.max(o, c) - 1e-9 || l > Math.min(o, c) + 1e-9) bad++;
    if (i) {
      const dt = t - rows[i - 1][0];
      if (dt !== ms) { gaps++; missing += Math.max(0, dt / ms - 1); }
      if (Math.abs(c / rows[i - 1][4] - 1) > 0.25) jumps++;
    }
  }
  return { file: path.basename(f), rows: rows.length, first: rows[0]?.[0], last: rows.at(-1)?.[0], gaps, missing, bad, jumps };
}

async function funding(inst) {
  const f = path.join(OUT, `${inst}_funding.csv.gz`);
  const got = new Map(readCsv(f).map((r) => [r[0], r]));
  let after = Date.now();
  for (let k = 0; k < 50; k++) {
    const d = await get("/api/v5/public/funding-rate-history", `instId=${inst}&after=${after}&limit=100`);
    await sleep(250);
    if (!d?.length) break;
    for (const x of d) got.set(+x.fundingTime, [+x.fundingTime, +x.realizedRate || +x.fundingRate]);
    after = +d.at(-1).fundingTime;
  }
  const rows = [...got.values()].sort((a, b) => a[0] - b[0]);
  writeCsv(f, "ts,rate", rows);
  return { file: path.basename(f), rows: rows.length, first: rows[0]?.[0], last: rows.at(-1)?.[0] };
}

fs.mkdirSync(OUT, { recursive: true });
const manifest = { source: "OKX", updatedAt: new Date().toISOString(), start: START_OF, files: [] };
for (const inst of INSTS) {
  for (const [bar, ms] of Object.entries(BARS)) {
    if (bar === "15m" && !INTRADAY.has(inst)) continue;
    const m = await candles(inst, bar, ms);
    console.log(JSON.stringify(m));
    manifest.files.push(m);
  }
  const fm = await funding(inst);
  console.log(JSON.stringify(fm));
  manifest.files.push(fm);
}
fs.writeFileSync(path.join(OUT, `manifest${process.argv[3] ? `-${process.argv[3]}` : ""}.json`), JSON.stringify(manifest, null, 2));
