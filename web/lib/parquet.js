// ============================================================
// web/lib/parquet.js — thin wrapper around hyparquet for the dashboard.
// ============================================================
//
// The pipeline is now API-free: Scala/Spark writes single-file parquet
// tables into `data/analytics/`, `data/scores/`, and `data/stream/`, and
// this module fetches them directly over the static HTTP server.
//
// Everything goes through `loadParquet(url)`, which returns plain JS arrays
// keyed by column name plus a row-oriented view. BigInts are coerced to
// regular numbers so Plotly.js can consume them without complaint.

// hyparquet is a 0-dep, pure-ESM library served via esm.sh so the page
// works without a build step.
const HYPARQUET_CDN = "https://esm.sh/hyparquet@1.25";

let hyparquetPromise = null;
function getHyparquet() {
  if (!hyparquetPromise) hyparquetPromise = import(HYPARQUET_CDN);
  return hyparquetPromise;
}

function toNumberIfBigInt(v) {
  // Parquet emits INT64 as BigInt; Plotly + Math can't handle BigInt.
  if (typeof v === "bigint") return Number(v);
  return v;
}

function sanitizeRow(row) {
  const out = {};
  for (const k of Object.keys(row)) out[k] = toNumberIfBigInt(row[k]);
  return out;
}

/**
 * Fetch a single-file parquet from `url` and return `{ rows, columns }`.
 * `rows` is an array of plain objects; `columns` is a map from column name
 * to an array of values in the same order as `rows`.
 */
export async function loadParquet(url) {
  const hq = await getHyparquet();
  const file = await hq.asyncBufferFromUrl({ url });
  const raw = await hq.parquetReadObjects({ file });
  const rows = raw.map(sanitizeRow);
  const columns = {};
  if (rows.length) {
    for (const k of Object.keys(rows[0])) {
      columns[k] = rows.map((r) => r[k]);
    }
  }
  return { rows, columns };
}

/**
 * Group rows by the value of `key`. Returns a Map<value, Row[]>, preserving
 * first-seen insertion order.
 */
export function groupBy(rows, key) {
  const out = new Map();
  for (const r of rows) {
    const k = r[key];
    if (!out.has(k)) out.set(k, []);
    out.get(k).push(r);
  }
  return out;
}

/**
 * Return a copy of `rows` with only the rows matching `predicate`.
 */
export function filterRows(rows, predicate) {
  return rows.filter(predicate);
}

/**
 * Sort `rows` by the numeric value of `key`. `direction` is "asc" or "desc".
 */
export function sortBy(rows, key, direction = "asc") {
  const sign = direction === "desc" ? -1 : 1;
  return rows.slice().sort((a, b) => {
    const av = a[key], bv = b[key];
    if (av == null && bv == null) return 0;
    if (av == null) return sign;
    if (bv == null) return -sign;
    return (av < bv ? -1 : av > bv ? 1 : 0) * sign;
  });
}
