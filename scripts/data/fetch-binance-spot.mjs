/**
 * BTC 현물 장기 1분봉 — 바이낸스 공개 파일(월별 + 이번 달 일별) BTCUSDT 1m, 2017-08 부터.
 * 출력: <outDir>/<SYM>-SPOT_1m.csv.gz  (ts,o,h,l,c,v · ts=ms UTC · 정렬·중복 제거)
 * 실행: node scripts/data/fetch-binance-spot.mjs <outDir> [SYM=BTCUSDT]
 */
import fs from "node:fs";
import path from "node:path";
import zlib from "node:zlib";
import os from "node:os";
import { execFileSync } from "node:child_process";

const OUT = process.argv[2];
const SYM = process.argv[3] ?? "BTCUSDT";
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "spot-"));
fs.mkdirSync(OUT, { recursive: true });
const gz = zlib.createGzip();
const ws = fs.createWriteStream(path.join(OUT, `${SYM}-SPOT_1m.csv.gz`));
gz.pipe(ws);
gz.write("ts,o,h,l,c,v\n");
let last = 0, count = 0, first = 0;
async function grab(url, name) {
  for (let k = 0; k < 3; k++) {
    const r = await fetch(url).catch(() => null);
    if (r && r.status === 404) return false;
    if (r && r.ok) {
      const zf = path.join(tmp, name);
      fs.writeFileSync(zf, Buffer.from(await r.arrayBuffer()));
      const csv = execFileSync("unzip", ["-p", zf], { maxBuffer: 1 << 30 }).toString();
      const buf = [];
      for (const line of csv.trim().split("\n")) {
        const f = line.split(",");
        let ts = Number(f[0]);
        if (!Number.isFinite(ts)) continue;
        if (ts > 1e14) ts = Math.floor(ts / 1000); // 2025년부터 마이크로초
        const o = +f[1], h = +f[2], l = +f[3], c = +f[4], v = +f[5];
        if (ts > last && [o, h, l, c].every((x) => x > 0)) { buf.push(`${ts},${o},${h},${l},${c},${v}`); last = ts; count++; if (!first) first = ts; }
      }
      if (buf.length && !gz.write(buf.join("\n") + "\n")) await new Promise((r) => gz.once("drain", r));
      fs.rmSync(zf);
      return true;
    }
    await new Promise((s) => setTimeout(s, 2000));
  }
  return false;
}
const now = new Date();
const curY = now.getUTCFullYear(), curM = now.getUTCMonth() + 1;
for (let y = 2017; y <= curY; y++) {
  for (let m = 1; m <= 12; m++) {
    if (y === 2017 && m < 8) continue;
    if (y === curY && m >= curM) break;
    const ym = `${y}-${String(m).padStart(2, "0")}`;
    const ok = await grab(`https://data.binance.vision/data/spot/monthly/klines/${SYM}/1m/${SYM}-1m-${ym}.zip`, `${ym}.zip`);
    console.log(ym, ok ? "ok" : "없음", count);
  }
}
for (let d = 1; d < now.getUTCDate(); d++) {
  const ds = `${curY}-${String(curM).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
  await grab(`https://data.binance.vision/data/spot/daily/klines/${SYM}/1m/${SYM}-1m-${ds}.zip`, `${ds}.zip`);
}
gz.end();
await new Promise((r) => ws.on("finish", r));
console.log("총", count, "봉", first, "~", last);
