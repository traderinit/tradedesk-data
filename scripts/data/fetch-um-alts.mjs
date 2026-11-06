/**
 * 바이낸스 USDT 무기한 전체(상장 폐지 포함) 1시간봉 — 조각(shard) 단위 수집
 * 실행: node fetch-um-alts.mjs <outDir> <shard> <nShards>  → <SYM>-UM_1h.csv.gz (fetch-binance-vision.mjs 사용)
 */
import { execFileSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const [OUT, SHARD, N] = process.argv.slice(2);
const S3 = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision";
async function list(prefix) {
  const out = [];
  let marker = "";
  for (;;) {
    const r = await fetch(`${S3}?prefix=${encodeURIComponent(prefix)}&delimiter=/${marker ? `&marker=${encodeURIComponent(marker)}` : ""}`);
    const x = await r.text();
    const ps = [...x.matchAll(/<Prefix>([^<]+)<\/Prefix>/g)].map((m) => m[1]).filter((p) => p !== prefix);
    out.push(...ps);
    if (!/<IsTruncated>true/.test(x)) break;
    marker = /<NextMarker>([^<]+)</.exec(x)?.[1] ?? out.at(-1);
  }
  return out;
}
const STABLE = /^(USDC|BUSD|TUSD|USDP|PAX|DAI|FDUSD|USDS|SUSD|EUR|GBP|AUD|TRY|BRL|UST|USTC|PAXG|AEUR|EURI|XUSD|USD1|BFUSD|RLUSD|U)USDT$/;
const syms = (await list("data/futures/um/monthly/klines/")).map((p) => p.split("/").at(-2)).filter((s) => s && /^[A-Z0-9]+USDT$/.test(s) && !STABLE.test(s)).sort();
const mine = syms.filter((_, i) => i % Number(N) === Number(SHARD));
console.log("전체", syms.length, "이 조각", mine.length);
const vision = path.join(path.dirname(fileURLToPath(import.meta.url)), "fetch-binance-vision.mjs");
for (const s of mine) {
  try {
    execFileSync("node", [vision, OUT, "um-klines", s, "1h", "2020-01"], { stdio: "inherit" });
  } catch (e) {
    console.log("실패", s);
  }
}
