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

function niceMax(value: number): number {
  const magnitude = 10 ** Math.floor(Math.log10(value));
  for (const m of [1, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10]) if (m * magnitude >= value) return m * magnitude;
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
  const yMax = niceMax(Math.max(...results.map((r) => r.max)));
  const y = (v: number) => TOP + ph - (v / yMax) * ph;

  const title = `Throughput peaks at ${num(peak.chunk)} rows per chunk: ${num(peak.median)} rows/s`;
  const sub1 = `Rows per second posting ${num(bench.config.rows)} transactions. Each chunk is one database transaction,`;
  const sub2 = `written together with its checkpoint. Median of ${bench.config.repeats} runs; the bars show the slowest to the fastest.`;
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
  const step = yMax / 4;
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
  for (const r of results) {
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
  const label = (r: Result, line1: string, line2: string) => {
    const px = x(r.chunk);
    const anchor = px > LEFT + pw - 90 ? "end" : px < LEFT + 90 ? "start" : "middle";
    const top = y(r.max) - 26;
    add(`<text x="${px.toFixed(1)}" y="${top.toFixed(1)}" font-size="12" font-weight="600" fill="${t.ink}" text-anchor="${anchor}">${line1}</text>`);
    add(`<text x="${px.toFixed(1)}" y="${(top + 14).toFixed(1)}" font-size="11" fill="${t.ink2}" text-anchor="${anchor}">${line2}</text>`);
  };
  label(slowest, `${num(slowest.median)} rows/s`, `${num(slowest.chunk)} per chunk`);
  label(peak, `${num(peak.median)} rows/s`, peak.chunk === DEFAULT_CHUNK ? "peak, and the default" : "peak");
  const def = results.find((r) => r.chunk === DEFAULT_CHUNK);
  if (def && def !== peak) label(def, `${num(def.median)} rows/s`, "the default");

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
