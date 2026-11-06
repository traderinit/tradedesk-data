/**
 * 시장 구조 데이터 전진 수집 (OI · 현물 · mark · index · 펀딩 스냅샷 · 상품 상태) + 자동 점검
 *  - 확정된 봉만, 시각순, 중복 제거. 빠진 값은 보간하지 않고 빈 채로 두고 점검 결과에 기록한다.
 *  - 재현성: 이미 받은 구간 일부를 매번 다시 받아 값이 바뀌었는지(revised) 센다. 파일마다 내용 해시(sha256)를 남긴다.
 *  - 스냅샷(jsonl)은 받은 원본 그대로 + 받은 시각(collectedAt)을 덧붙여 쌓는다 (지우지 않음).
 *  - 이 데이터는 쌓이는 동안 전략 백테스트에 쓰지 않는다 — 먼저 수집 정확성만 확인한다.
 * 실행: node scripts/data/fetch-okx-market.mjs <outDir> <COIN>
 */
import fs from "node:fs";
import path from "node:path";
import zlib from "node:zlib";
import crypto from "node:crypto";

const OUT = process.argv[2];
const COIN = process.argv[3];
if (!OUT || !COIN) throw new Error("usage: fetch-okx-market.mjs <outDir> <COIN>");
const SWAP = `${COIN}-USDT-SWAP`;
const SPOT = `${COIN}-USDT`;
const H = 3_600_000;
const NOW = Date.now();
const collectedAt = new Date(NOW).toISOString();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function get(p, qs) {
  for (let i = 0; i < 6; i++) {
    try {
      const r = await fetch(`https://www.okx.com${p}?${qs}`, { headers: { accept: "application/json" } });
      const j = await r.json();
      if (j.code === "0") return j.data;
      if (j.code === "50011") { await sleep(2000); continue; }
      const e = new Error(`${p} ${j.code} ${j.msg}`);
      e.okx = j.code;
      throw e;
    } catch (e) {
      if (e.okx && e.okx !== "50011") throw e; // 없는 상품 등은 재시도하지 않음
      if (i === 5) throw e;
      await sleep(1000 * (i + 1));
    }
  }
}

const readCsv = (f) => (fs.existsSync(f) ? zlib.gunzipSync(fs.readFileSync(f)).toString().trim().split("\n").slice(1).filter(Boolean).map((l) => l.split(",").map(Number)) : []);
const writeCsv = (f, head, rows) => {
  const txt = head + "\n" + rows.map((r) => r.join(",")).join("\n") + "\n";
  fs.writeFileSync(f, zlib.gzipSync(txt, { level: 9 }));
  return crypto.createHash("sha256").update(txt).digest("hex");
};
const appendJsonl = (f, obj) => fs.appendFileSync(f, JSON.stringify(obj) + "\n");
const same = (a, b) => a.length === b.length && a.every((v, i) => v === b[i] || Math.abs(v - b[i]) <= 1e-12 * Math.max(1, Math.abs(v)));

/** 봉 시계열 공통 점검: 정렬·중복·시각 정렬(봉 경계)·빠진 봉·가격 이상·급변 */
function checkSeries(rows, ms, { price = true, jump = 0.25 } = {}) {
  let misaligned = 0, gaps = 0, missing = 0, bad = 0, jumps = 0, dup = 0;
  for (let i = 0; i < rows.length; i++) {
    const [t, ...v] = rows[i];
    if (t % ms !== 0) misaligned++;
    if (price) {
      const [o, h, l, c] = v;
      if (!(o > 0 && h > 0 && l > 0 && c > 0) || h < l || h < Math.max(o, c) - 1e-9 || l > Math.min(o, c) + 1e-9) bad++;
    } else if (!(v[0] >= 0)) bad++;
    if (i) {
      const dt = t - rows[i - 1][0];
      if (dt === 0) dup++;
      else if (dt !== ms) { gaps++; missing += Math.max(0, dt / ms - 1); }
      const a = price ? rows[i - 1][4] : rows[i - 1][1];
      const b = price ? v[3] : v[0];
      if (a > 0 && Math.abs(b / a - 1) > jump) jumps++;
    }
  }
  return { rows: rows.length, first: rows[0]?.[0] ?? null, last: rows.at(-1)?.[0] ?? null, misaligned, dup, gaps, missing, bad, jumps };
}

/**
 * 1시간봉 이어 받기 (확정봉만). 끝에서 overlap 개를 다시 받아 기존 값과 비교 → revised.
 * kind: candles(현물) · mark · index
 */
async function bars1h(file, endpoint, instId, start, head, map, { overlap = 48 } = {}) {
  const f = path.join(OUT, file);
  const have = readCsv(f);
  const got = new Map(have.map((r) => [r[0], r]));
  // 첫 실행은 끝(또는 상장 시점)까지, 이후는 마지막 봉 앞 overlap 개부터
  const from = have.length ? have.at(-1)[0] - overlap * H : start - H;
  let after = NOW, revised = 0, refetched = 0;
  for (let k = 0; k < 2000; k++) {
    const d = await get(endpoint, `instId=${instId}&bar=1H&after=${after}&limit=100`);
    await sleep(120);
    if (!d?.length) break;
    for (const x of d) {
      const row = map(x);
      if (!row) continue;
      const old = got.get(row[0]);
      if (old) { refetched++; if (!same(old, row)) revised++; }
      got.set(row[0], row);
    }
    const oldest = +d.at(-1)[0];
    if (oldest <= from || oldest <= start) break;
    after = oldest;
  }
  const rows = [...got.values()].filter((r) => r[0] >= start).sort((a, b) => a[0] - b[0]);
  const sha256 = writeCsv(f, head, rows);
  return { file, sha256, refetched, revised, ...checkSeries(rows, H) };
}

/** OI 1시간 이력 — OKX 는 최근 1,440개(약 60일)만 준다. 매번 전부 다시 받아 기존 값과 비교. */
async function oi1h() {
  const file = `${SWAP}_oi1H.csv.gz`;
  const f = path.join(OUT, file);
  const have = readCsv(f);
  const got = new Map(have.map((r) => [r[0], r]));
  let end = NOW, revised = 0, refetched = 0, fetched = 0;
  for (let k = 0; k < 20; k++) {
    const d = await get("/api/v5/rubik/stat/contracts/open-interest-history", `instId=${SWAP}&period=1H&end=${end}&limit=100`);
    await sleep(250);
    if (!d?.length) break;
    for (const x of d) {
      // [ts, oi(계약 수), oiCcy(코인 수), oiUsd]
      const row = [+x[0], +x[1], +x[2], +x[3]];
      fetched++;
      const old = got.get(row[0]);
      if (old) { refetched++; if (!same(old, row)) revised++; }
      got.set(row[0], row);
    }
    const oldest = +d.at(-1)[0];
    if (oldest >= end) break;
    end = oldest;
  }
  const rows = [...got.values()].sort((a, b) => a[0] - b[0]);
  const sha256 = writeCsv(f, "ts,oi,oiCcy,oiUsd", rows);
  // 단위 급변: 1시간 사이 OI(코인 수)가 5배 이상 늘거나 1/5 이하로 줄면 표시
  const unitJumps = rows.filter((r, i) => i && rows[i - 1][2] > 0 && (r[2] / rows[i - 1][2] > 5 || r[2] / rows[i - 1][2] < 0.2)).map((r) => r[0]);
  return { file, sha256, fetched, refetched, revised, unitJumps: unitJumps.slice(0, 20), ...checkSeries(rows.map((r) => [r[0], r[2]]), H, { price: false, jump: 0.5 }) };
}

const res = { coin: COIN, collectedAt, files: [], snapshots: {}, checks: {}, errors: [] };
const step = async (name, fn) => {
  try { return await fn(); } catch (e) { res.errors.push({ step: name, error: String(e.message ?? e) }); console.error(name, e.message ?? e); return null; }
};
fs.mkdirSync(OUT, { recursive: true });

// 1) 상품 정보 스냅샷 — 상장 시각·상태·계약 크기 (OI 단위 환산용)
const inst = await step("instruments", async () => {
  const sw = (await get("/api/v5/public/instruments", `instType=SWAP&instId=${SWAP}`))?.[0] ?? null;
  const sp = await get("/api/v5/public/instruments", `instType=SPOT&instId=${SPOT}`).then((d) => d?.[0] ?? null).catch(() => null);
  const snap = { collectedAt, swap: sw, spot: sp };
  appendJsonl(path.join(OUT, `${COIN}-instruments.jsonl`), snap);
  res.snapshots.instruments = {
    swapState: sw?.state ?? null, swapListTime: sw ? +sw.listTime : null, ctVal: sw ? +sw.ctVal : null, ctValCcy: sw?.ctValCcy ?? null,
    spotState: sp?.state ?? null, spotListTime: sp ? +sp.listTime : null,
  };
  return snap;
});

// 2) 펀딩 현재값 스냅샷 — 정산 시각·다음 정산·상하한·산정 방식 (정산 주기 변경 추적)
await step("funding-snapshot", async () => {
  const fr = (await get("/api/v5/public/funding-rate", `instId=${SWAP}`))?.[0] ?? null;
  appendJsonl(path.join(OUT, `${SWAP}_funding-snap.jsonl`), { collectedAt, data: fr });
  if (fr) res.snapshots.funding = { fundingTime: +fr.fundingTime, nextFundingTime: +fr.nextFundingTime, intervalH: (+fr.nextFundingTime - +fr.fundingTime) / H, method: fr.method ?? null, max: +fr.maxFundingRate || null, min: +fr.minFundingRate || null };
});

// 3) OI 1시간 (최근 약 60일만 제공 → 매일 이어 붙여 보존)
const oi = await step("oi1H", oi1h);
if (oi) res.files.push(oi);

// 4) 현물 1시간 (상장 시점부터) · 5) index 1시간 · 6) mark 1시간 (2022년부터)
const spot = await step("spot1H", () => bars1h(`${SPOT}_1H.csv.gz`, "/api/v5/market/history-candles", SPOT, Date.UTC(2019, 0, 1), "ts,o,h,l,c,vol",
  (x) => (x[8] === "1" ? [+x[0], +x[1], +x[2], +x[3], +x[4], +x[5]] : null)));
if (spot) res.files.push(spot);
const index = await step("index1H", () => bars1h(`${SPOT}_index1H.csv.gz`, "/api/v5/market/history-index-candles", SPOT, Date.UTC(2022, 0, 1), "ts,o,h,l,c",
  (x) => (x[5] === "1" ? [+x[0], +x[1], +x[2], +x[3], +x[4]] : null)));
if (index) res.files.push(index);
const mark = await step("mark1H", () => bars1h(`${SWAP}_mark1H.csv.gz`, "/api/v5/market/history-mark-price-candles", SWAP, Date.UTC(2022, 0, 1), "ts,o,h,l,c",
  (x) => (x[5] === "1" ? [+x[0], +x[1], +x[2], +x[3], +x[4]] : null)));
if (mark) res.files.push(mark);

// 7) 교차 점검
await step("cross-checks", async () => {
  const perp = new Map(readCsv(path.join(OUT, `${SWAP}_1H.csv.gz`)).map((r) => [r[0], r[4]]));
  const sp = new Map(readCsv(path.join(OUT, `${SPOT}_1H.csv.gz`)).map((r) => [r[0], r[4]]));
  const ix = new Map(readCsv(path.join(OUT, `${SPOT}_index1H.csv.gz`)).map((r) => [r[0], r[4]]));
  const mk = new Map(readCsv(path.join(OUT, `${SWAP}_mark1H.csv.gz`)).map((r) => [r[0], r[4]]));
  const pct = (xs, q) => { if (!xs.length) return null; const s = [...xs].sort((a, b) => a - b); return s[Math.min(s.length - 1, Math.floor(q * s.length))]; };
  /** 같은 1시간봉 종가끼리 비교 (같은 거래소 · 같은 시각) */
  const cmp = (a, b, warn) => {
    const d = [];
    let n = 0, over = 0;
    for (const [t, x] of a) { const y = b.get(t); if (y > 0 && x > 0) { n++; const v = x / y - 1; d.push(v); if (Math.abs(v) > warn) over++; } }
    return { n, p01: pct(d, 0.01), p50: pct(d, 0.5), p99: pct(d, 0.99), over, warn };
  };
  res.checks.basisPerpSpot = cmp(perp, sp, 0.05); // 무기한 종가 / 현물 종가 - 1
  res.checks.indexVsSpot = cmp(ix, sp, 0.02); // OKX 지수 / OKX 현물
  res.checks.markVsIndex = cmp(mk, ix, 0.03);
  // OI 단위: oiCcy ≈ oi × ctVal (코인 단위 계약일 때)
  const iv = res.snapshots.instruments;
  const oiRows = readCsv(path.join(OUT, `${SWAP}_oi1H.csv.gz`));
  if (iv?.ctVal && iv.ctValCcy === COIN && oiRows.length) {
    const bad = oiRows.filter((r) => r[1] > 0 && Math.abs((r[1] * iv.ctVal) / r[2] - 1) > 0.01).length;
    res.checks.oiUnit = { rows: oiRows.length, ctVal: iv.ctVal, mismatch: bad };
  }
  // 상장 구간: 상장 이전 데이터가 있으면 이상
  if (iv?.swapListTime) res.checks.beforeListingPerp = [...perp.keys()].filter((t) => t < iv.swapListTime - H).length;
  if (iv?.spotListTime) res.checks.beforeListingSpot = [...sp.keys()].filter((t) => t < iv.spotListTime - H).length;
  // 펀딩 정산 시각: 정시 정렬 · 간격 분포 (주기 변경 추적)
  const fr = readCsv(path.join(OUT, `${SWAP}_funding.csv.gz`));
  const gaps = {};
  let offHour = 0;
  for (let i = 0; i < fr.length; i++) {
    if (fr[i][0] % H !== 0) offHour++;
    if (i) { const g = (fr[i][0] - fr[i - 1][0]) / H; gaps[g] = (gaps[g] ?? 0) + 1; }
  }
  res.checks.funding = { rows: fr.length, offHour, intervalHours: gaps };
});

// 경고 요약 — 무엇이든 0 이 아니면 사람이 볼 것
const warn = [];
for (const f of res.files) {
  if (f.misaligned) warn.push(`${f.file}: 봉 경계 어긋남 ${f.misaligned}`);
  if (f.dup) warn.push(`${f.file}: 중복 ${f.dup}`);
  if (f.bad) warn.push(`${f.file}: 가격·값 이상 ${f.bad}`);
  if (f.revised) warn.push(`${f.file}: 다시 받은 값 변경 ${f.revised}/${f.refetched}`);
  if (f.unitJumps?.length) warn.push(`${f.file}: OI 단위 급변 의심 ${f.unitJumps.length}`);
}
const c = res.checks;
if (c.oiUnit?.mismatch) warn.push(`OI 단위 불일치 ${c.oiUnit.mismatch}/${c.oiUnit.rows}`);
if (c.beforeListingPerp) warn.push(`무기한 상장 전 데이터 ${c.beforeListingPerp}`);
// 현물 listTime 은 OKX 메타 갱신 시각(예: BTC 2021-01-29)이라 실제 상장보다 늦다 — 경고가 아니라 참고값으로만 남긴다
if (c.beforeListingSpot) c.beforeListingSpotNote = "OKX 현물 listTime 은 실제 상장 시각이 아닐 수 있음 (참고값)";
if (c.funding?.offHour) warn.push(`펀딩 정산 시각 정시 아님 ${c.funding.offHour}`);
if (c.indexVsSpot?.over) warn.push(`지수-현물 2% 초과 ${c.indexVsSpot.over}`);
if (c.basisPerpSpot?.over) warn.push(`무기한-현물 5% 초과 ${c.basisPerpSpot.over}`);
if (res.snapshots.instruments && res.snapshots.instruments.swapState !== "live") warn.push(`무기한 상태 ${res.snapshots.instruments.swapState}`);
res.warnings = warn;
fs.writeFileSync(path.join(OUT, `manifest-market-${COIN}.json`), JSON.stringify(res, null, 2));
console.log(JSON.stringify({ coin: COIN, files: res.files.map((f) => [f.file, f.rows, f.missing, f.revised]), warnings: warn, errors: res.errors }));
