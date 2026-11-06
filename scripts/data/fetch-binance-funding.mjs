/**
 * 과거 펀딩비 보강 — OKX 는 최근 3개월만 주므로, 같은 코인의 바이낸스 USDT 무기한 펀딩비(월별 공개 파일)를 받아
 * <outDir>/<INST>_funding_bn.csv.gz 로 저장한다 (거래소가 달라 근사값 — 백테스트는 OKX 실제 값을 먼저 쓴다).
 * 실행: node scripts/data/fetch-binance-funding.mjs <outDir> <COIN>
 */
import fs from "node:fs";
import path from "node:path";
import zlib from "node:zlib";
import { execFileSync } from "node:child_process";
import os from "node:os";

const OUT = process.argv[2];
const COIN = process.argv[3];
const inst = `${COIN}-USDT-SWAP`;
const sym = `${COIN}USDT`;
const rows = new Map();
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "fund-"));
const now = new Date();
for (let y = 2019; y <= now.getUTCFullYear(); y++) {
  for (let m = 1; m <= 12; m++) {
    if (y === now.getUTCFullYear() && m > now.getUTCMonth() + 1) break;
    const ym = `${y}-${String(m).padStart(2, "0")}`;
    const url = `https://data.binance.vision/data/futures/um/monthly/fundingRate/${sym}/${sym}-fundingRate-${ym}.zip`;
    const r = await fetch(url).catch(() => null);
    if (!r || !r.ok) continue;
    const zf = path.join(tmp, `${ym}.zip`);
    fs.writeFileSync(zf, Buffer.from(await r.arrayBuffer()));
    const csv = execFileSync("unzip", ["-p", zf]).toString();
    for (const line of csv.trim().split("\n")) {
      const [t, , rate] = line.split(",");
      const ts = Number(t);
      const v = Number(rate);
      if (Number.isFinite(ts) && Number.isFinite(v)) rows.set(ts, v);
    }
  }
}
// 정산 시각을 정시(1시간 경계)로 맞춘다 (바이낸스 기록은 몇 ms 어긋남). 정산 간격(8시간·4시간)은 그대로 둔다
const byHour = new Map();
for (const [t, v] of rows) byHour.set(Math.round(t / 3_600_000) * 3_600_000, v);
const out = [...byHour].sort((a, b) => a[0] - b[0]);
fs.writeFileSync(path.join(OUT, `${inst}_funding_bn.csv.gz`), zlib.gzipSync("ts,rate\n" + out.map((r) => r.join(",")).join("\n") + "\n", { level: 9 }));
console.log(JSON.stringify({ file: `${inst}_funding_bn.csv.gz`, rows: out.length, first: out[0]?.[0], last: out.at(-1)?.[0] }));
