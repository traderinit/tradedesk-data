/**
 * 바이낸스 공개 파일(data.binance.vision) 수집 — 월별 + 이번 달 일별, 스트리밍 저장
 * 실행: node fetch-binance-vision.mjs <outDir> <kind> <SYM> [interval=1m] [start=YYYY-MM]
 *   kind: spot-klines | um-klines → <SYM>-<SPOT|UM>_<interval>.csv.gz  (ts,o,h,l,c,v,tbv · tbv = 시장가 매수 체결량)
 *         um-metrics → <SYM>-UM_metrics.csv.gz (ts,oi,oiv,top_ls_acc,top_ls_pos,ls_acc,taker_ls · 5분, 2021-12~, 일별 파일만)
 */
import fs from "node:fs";
import path from "node:path";
import zlib from "node:zlib";
import os from "node:os";
import { execFileSync } from "node:child_process";

const [OUT, KIND, SYM, IV = "1m", START = "2017-08"] = process.argv.slice(2);
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "bv-"));
fs.mkdirSync(OUT, { recursive: true });
const name = KIND === "um-metrics" ? `${SYM}-UM_metrics.csv.gz` : `${SYM}-${KIND === "spot-klines" ? "SPOT" : "UM"}_${IV}.csv.gz`;
const gz = zlib.createGzip();
const ws = fs.createWriteStream(path.join(OUT, name));
gz.pipe(ws);
gz.write(KIND === "um-metrics" ? "ts,oi,oiv,top_ls_acc,top_ls_pos,ls_acc,taker_ls\n" : "ts,o,h,l,c,v,tbv\n");
let last = 0, count = 0;
const base = KIND === "spot-klines" ? "spot" : "futures/um";
async function grab(url, file) {
  for (let k = 0; k < 3; k++) {
    const r = await fetch(url).catch(() => null);
    if (r && r.status === 404) return false;
    if (r && r.ok) {
      const zf = path.join(tmp, file);
      fs.writeFileSync(zf, Buffer.from(await r.arrayBuffer()));
      const csv = execFileSync("unzip", ["-p", zf], { maxBuffer: 1 << 30 }).toString();
      fs.rmSync(zf);
      const buf = [];
      for (const line of csv.trim().split("\n")) {
        const f = line.split(",");
        if (KIND === "um-metrics") {
          const ts = Date.parse(f[0].replace(" ", "T") + "Z");
          if (!Number.isFinite(ts) || ts <= last) continue;
          buf.push([ts, f[2], f[3], f[4], f[5], f[6], f[7]].join(","));
          last = ts;
        } else {
          let ts = Number(f[0]);
          if (!Number.isFinite(ts)) continue;
          if (ts > 1e14) ts = Math.floor(ts / 1000);
          if (ts <= last || !(+f[4] > 0)) continue;
          buf.push([ts, f[1], f[2], f[3], f[4], f[5], f[9]].join(","));
          last = ts;
        }
        count++;
      }
      if (buf.length && !gz.write(buf.join("\n") + "\n")) await new Promise((res) => gz.once("drain", res));
      return true;
    }
    await new Promise((s) => setTimeout(s, 2000));
  }
  return false;
}
const now = new Date();
const curY = now.getUTCFullYear(), curM = now.getUTCMonth() + 1;
const [sy, sm] = START.split("-").map(Number);
const pad = (x) => String(x).padStart(2, "0");
if (KIND === "um-metrics") {
  for (let t = Date.UTC(sy, sm - 1, 1); t < Date.UTC(curY, curM - 1, now.getUTCDate()); t += 86_400_000) {
    const ds = new Date(t).toISOString().slice(0, 10);
    await grab(`https://data.binance.vision/data/futures/um/daily/metrics/${SYM}/${SYM}-metrics-${ds}.zip`, `${ds}.zip`);
    if (ds.endsWith("-01")) console.log(ds, count);
  }
} else {
  for (let y = sy; y <= curY; y++)
    for (let m = y === sy ? sm : 1; m <= 12; m++) {
      if (y === curY && m >= curM) break;
      const ym = `${y}-${pad(m)}`;
      const ok = await grab(`https://data.binance.vision/data/${base}/monthly/klines/${SYM}/${IV}/${SYM}-${IV}-${ym}.zip`, `${ym}.zip`);
      if (m === 1) console.log(ym, ok, count);
    }
  for (let d = 1; d < now.getUTCDate(); d++) {
    const ds = `${curY}-${pad(curM)}-${pad(d)}`;
    await grab(`https://data.binance.vision/data/${base}/daily/klines/${SYM}/${IV}/${SYM}-${IV}-${ds}.zip`, `${ds}.zip`);
  }
}
gz.end();
await new Promise((r) => ws.on("finish", r));
console.log(name, "총", count, "~", new Date(last).toISOString());
