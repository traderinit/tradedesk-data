/**
 * 바이낸스 현물 USDT 페어 전체(상장 폐지 포함) 일봉 — 코인 간 비교(CTREND 등)용
 * 출력: <outDir>/SPOT_1d_universe.csv.gz (sym,ts,o,h,l,c,v,qv,tbv) · 레버리지 토큰·스테이블 제외
 * 실행: node fetch-binance-universe.mjs <outDir> [spot|um]  (um = USDT 무기한 일봉 + 펀딩비 → UM_1d_universe.csv.gz · UM_funding_universe.csv.gz)
 */
import fs from "node:fs";
import path from "node:path";
import zlib from "node:zlib";
import os from "node:os";
import { execFileSync } from "node:child_process";

const OUT = process.argv[2];
const MK = process.argv[3] ?? "spot";
const BASE = MK === "um" ? "data/futures/um" : "data/spot";
fs.mkdirSync(OUT, { recursive: true });
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "uni-"));
const S3 = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision";
async function list(prefix) {
  const out = [];
  let marker = "";
  for (;;) {
    const r = await fetch(`${S3}?prefix=${encodeURIComponent(prefix)}&delimiter=/${marker ? `&marker=${encodeURIComponent(marker)}` : ""}`);
    const x = await r.text();
    const ps = [...x.matchAll(/<Prefix>([^<]+)<\/Prefix>/g)].map((m) => m[1]).filter((p) => p !== prefix);
    const ks = [...x.matchAll(/<Key>([^<]+)<\/Key>/g)].map((m) => m[1]);
    out.push(...ps, ...ks);
    if (!/<IsTruncated>true/.test(x)) break;
    marker = /<NextMarker>([^<]+)</.exec(x)?.[1] ?? out.at(-1);
  }
  return out;
}
const STABLE = /^(USDC|BUSD|TUSD|USDP|PAX|DAI|FDUSD|USDS|SUSD|EUR|GBP|AUD|TRY|BRL|UST|USTC|PAXG|AEUR|EURI|XUSD|USD1|BFUSD|RLUSD|U)USDT$/;
const LEV = /(UP|DOWN|BULL|BEAR)USDT$/;
const syms = (await list(`${BASE}/monthly/klines/`)).map((p) => p.split("/").at(-2)).filter((s) => s && s.endsWith("USDT") && !STABLE.test(s) && !LEV.test(s));
console.log("심볼", syms.length);
const gz = zlib.createGzip();
const ws = fs.createWriteStream(path.join(OUT, MK === "um" ? "UM_1d_universe.csv.gz" : "SPOT_1d_universe.csv.gz"));
const fgz = zlib.createGzip();
const fws = MK === "um" ? fs.createWriteStream(path.join(OUT, "UM_funding_universe.csv.gz")) : null;
if (fws) { fgz.pipe(fws); fgz.write("sym,ts,rate\n"); }
gz.pipe(ws);
gz.write("sym,ts,o,h,l,c,v,qv,tbv\n");
let done = 0, rows = 0;
async function one(sym) {
  const keys = (await list(`${BASE}/monthly/klines/${sym}/1d/`)).filter((k) => k.endsWith(".zip"));
  const buf = [];
  for (const k of keys) {
    for (let a = 0; a < 3; a++) {
      const r = await fetch(`https://data.binance.vision/${k}`).catch(() => null);
      if (r && r.ok) {
        const zf = path.join(tmp, `${sym}-${path.basename(k)}`);
        fs.writeFileSync(zf, Buffer.from(await r.arrayBuffer()));
        const csv = execFileSync("unzip", ["-p", zf]).toString();
        fs.rmSync(zf);
        for (const line of csv.trim().split("\n")) {
          const f = line.split(",");
          let ts = Number(f[0]);
          if (!Number.isFinite(ts)) continue;
          if (ts > 1e14) ts = Math.floor(ts / 1000);
          buf.push([sym, ts, f[1], f[2], f[3], f[4], f[5], f[7], f[9]].join(","));
        }
        break;
      }
      if (r && r.status === 404) break;
      await new Promise((s) => setTimeout(s, 1500));
    }
  }
  if (MK === "um") {
    const fk = (await list(`${BASE}/monthly/fundingRate/${sym}/`)).filter((k) => k.endsWith(".zip"));
    const fb = [];
    for (const k of fk) {
      const r = await fetch(`https://data.binance.vision/${k}`).catch(() => null);
      if (!r || !r.ok) continue;
      const zf = path.join(tmp, `${sym}-f-${path.basename(k)}`);
      fs.writeFileSync(zf, Buffer.from(await r.arrayBuffer()));
      const csv = execFileSync("unzip", ["-p", zf]).toString();
      fs.rmSync(zf);
      for (const line of csv.trim().split("\n")) {
        const f = line.split(",");
        const ts = Number(f[0]);
        if (Number.isFinite(ts)) fb.push([sym, ts, f[2]].join(","));
      }
    }
    if (fb.length) fgz.write(fb.join("\n") + "\n");
  }
  rows += buf.length;
  if (buf.length && !gz.write(buf.join("\n") + "\n")) await new Promise((r) => gz.once("drain", r));
  if (++done % 50 === 0) console.log(done, "/", syms.length, rows);
}
const q = [...syms];
await Promise.all(Array.from({ length: 12 }, async () => { while (q.length) await one(q.shift()); }));
gz.end();
await new Promise((r) => ws.on("finish", r));
if (fws) { fgz.end(); await new Promise((r) => fws.on("finish", r)); }
console.log("완료", syms.length, "심볼", rows, "행");
