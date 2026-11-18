// Draws throughput against chunk size from a benchmark results file, in light and dark versions.
//
//   node tools/chart.ts [docs/results/bench-latest.json]
//
// Writes bench-chart-light.svg and bench-chart-dark.svg next to the results file. No dependencies.
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";

interface Result {
  chunk: number;
  median: number;
  min: number;
  max: number;
}
interface Bench {
  config: { rows: number; repeats: number };
  results: Result[];
}

const THEMES = {
  light: { surface: "#fcfcfb", ink: "#0b0b0b", ink2: "#52514e", muted: "#898781", grid: "#e1e0d9", axis: "#c3c2b7", series: "#2a78d6" },
  dark: { surface: "#1a1a19", ink: "#f0efec", ink2: "#c3c2b7", muted: "#898781", grid: "#2c2c2a", axis: "#383835", series: "#3987e5" },
};
type Theme = (typeof THEMES)["light"];

const W = 820;
const H = 400;
const [LEFT, RIGHT, TOP, BOTTOM] = [64, 48, 104, 56];
const FONT = `system-ui, -apple-system, "Segoe UI", sans-serif`;
const DEFAULT_CHUNK = 500;
const num = (n: number) => n.toLocaleString("en");
const compact = (n: number) => (n >= 1000 ? `${n / 1000}K` : String(n));

/** A round tick step that covers `value` in at most four steps. */
function niceStep(value: number): number {
  const raw = value / 4;
  const magnitude = 10 ** Math.floor(Math.log10(raw));
  for (const m of [1, 2, 2.5, 3, 4, 5, 10]) if (m * magnitude >= raw) return m * magnitude;
  return 10 * magnitude;
}

function draw(bench: Bench, t: Theme): string {
  const results = [...bench.results].sort((a, b) => a.chunk - b.chunk);
  const peak = results.reduce((best, r) => (r.median > best.median ? r : best));
  const slowest = results[0]!;
  const pw = W - LEFT - RIGHT;
  const ph = H - TOP - BOTTOM;

  const lo = Math.log10(slowest.chunk) - 0.15;
  const hi = Math.log10(results.at(-1)!.chunk) + 0.15;
  const x = (chunk: number) => LEFT + ((Math.log10(chunk) - lo) / (hi - lo)) * pw;
  const step = niceStep(Math.max(...results.map((r) => r.max)));
  const yMax = step * Math.ceil(Math.max(...results.map((r) => r.max)) / step);
  const y = (v: number) => TOP + ph - (v / yMax) * ph;

  const largest = results.at(-1)!;
  // Only call it a peak if it is one; if the fastest size is the largest tested, the curve is still rising.
  const title =
    peak === largest
      ? `Bigger chunks are faster, with diminishing returns: ${Math.round(largest.median / slowest.median)}x from ${num(slowest.chunk)} to ${num(largest.chunk)} rows`
      : `Throughput peaks at ${num(peak.chunk)} rows per chunk: ${num(peak.median)} rows/s`;
  const sub1 = `Rows per second posting ${num(bench.config.rows)} transactions. Each chunk is one database transaction,`;
  // Spread bars only earn their place if the runs disagreed enough to see; otherwise say so in words.
  const spread = Math.max(...results.map((r) => (r.max - r.min) / r.median));
  const showSpread = spread >= 0.05;
  const sub2 = showSpread
    ? `written together with its checkpoint. Median of ${bench.config.repeats} runs; the bars show the slowest to the fastest.`
    : `written together with its checkpoint. Median of ${bench.config.repeats} runs, which differed by under ${Math.ceil(spread * 100)}%.`;
  const desc =
    `Throughput against chunk size on a log scale. ` +
    results.map((r) => `${num(r.chunk)} rows per chunk: ${num(r.median)} rows per second`).join("; ") + ".";

  const out: string[] = [];
  const add = (s: string) => out.push(s);
  add(`<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" font-family='${FONT}' role="img" aria-labelledby="title desc">`);
  add(`<title id="title">${title}</title><desc id="desc">${desc}</desc>`);
  add(`<rect width="${W}" height="${H}" rx="8" fill="${t.surface}"/>`);
  add(`<text x="${LEFT}" y="34" font-size="17" font-weight="600" fill="${t.ink}">${title}</text>`);
  add(`<text x="${LEFT}" y="56" font-size="13" fill="${t.ink2}">${sub1}</text>`);
  add(`<text x="${LEFT}" y="74" font-size="13" fill="${t.ink2}">${sub2}</text>`);

  // Gridlines and y ticks.
  for (let v = 0; v <= yMax + 1e-9; v += step) {
    add(`<line x1="${LEFT}" y1="${y(v).toFixed(1)}" x2="${LEFT + pw}" y2="${y(v).toFixed(1)}" stroke="${v === 0 ? t.axis : t.grid}" stroke-width="1"/>`);
    add(`<text x="${LEFT - 8}" y="${(y(v) + 4).toFixed(1)}" font-size="11" fill="${t.muted}" text-anchor="end" style="font-variant-numeric: tabular-nums">${compact(v)}</text>`);
  }
  // X ticks at every measured size.
  for (const r of results) {
    add(`<text x="${x(r.chunk).toFixed(1)}" y="${TOP + ph + 18}" font-size="11" fill="${t.muted}" text-anchor="middle" style="font-variant-numeric: tabular-nums">${num(r.chunk)}</text>`);
  }
  add(`<text x="${LEFT + pw / 2}" y="${H - 12}" font-size="11" fill="${t.muted}" text-anchor="middle">rows per chunk (log scale)</text>`);

  // Spread of the runs, then the median line, then the points on top.
  for (const r of showSpread ? results : []) {
    add(`<line x1="${x(r.chunk).toFixed(1)}" y1="${y(r.min).toFixed(1)}" x2="${x(r.chunk).toFixed(1)}" y2="${y(r.max).toFixed(1)}" stroke="${t.series}" stroke-opacity="0.35" stroke-width="6" stroke-linecap="round"/>`);
  }
  const d = results.map((r, i) => `${i ? "L" : "M"}${x(r.chunk).toFixed(1)},${y(r.median).toFixed(1)}`).join(" ");
  add(`<path d="${d}" fill="none" stroke="${t.series}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>`);
  for (const r of results) {
    add(
      `<circle cx="${x(r.chunk).toFixed(1)}" cy="${y(r.median).toFixed(1)}" r="4" fill="${t.series}" stroke="${t.surface}" stroke-width="2">` +
        `<title>${num(r.chunk)} rows per chunk: ${num(r.median)} rows/s (runs from ${num(r.min)} to ${num(r.max)})</title></circle>`,
    );
  }

  // Direct labels: the slowest size, the peak, and the default if it isn't the peak.
  // The line rises left to right, so a label above a point can run into the next segment; `below` avoids that.
  const label = (r: Result, line1: string, line2: string, below = false) => {
    // Below means below and to the right, where a rising line leaves room.
    const px = below ? x(r.chunk) + 10 : x(r.chunk);
    const anchor = below ? "start" : px > LEFT + pw - 90 ? "end" : px < LEFT + 90 ? "start" : "middle";
    const top = below ? y(r.min) + 22 : y(r.max) - 26;
    add(`<text x="${px.toFixed(1)}" y="${top.toFixed(1)}" font-size="12" font-weight="600" fill="${t.ink}" text-anchor="${anchor}">${line1}</text>`);
    add(`<text x="${px.toFixed(1)}" y="${(top + 14).toFixed(1)}" font-size="11" fill="${t.ink2}" text-anchor="${anchor}">${line2}</text>`);
  };
  label(slowest, `${num(slowest.median)} rows/s`, `${num(slowest.chunk)} per chunk`);
  const peakName = peak === largest ? "fastest tested" : "peak";
  label(peak, `${num(peak.median)} rows/s`, peak.chunk === DEFAULT_CHUNK ? `${peakName}, and the default` : `${num(peak.chunk)} per chunk, ${peakName}`);
  const def = results.find((r) => r.chunk === DEFAULT_CHUNK);
  if (def && def !== peak) label(def, `${num(def.median)} rows/s`, "500 per chunk, the default", true);

  add("</svg>");
  return out.join("\n") + "\n";
}

const path = process.argv[2] ?? "docs/results/bench-latest.json";
const bench = JSON.parse(readFileSync(path, "utf8")) as Bench;
for (const [name, theme] of Object.entries(THEMES)) {
  const target = join(dirname(path), `bench-chart-${name}.svg`);
  writeFileSync(target, draw(bench, theme));
  console.log(`Wrote ${target}`);
}
