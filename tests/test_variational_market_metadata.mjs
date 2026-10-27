import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import vm from "node:vm";

const source = await readFile(new URL("../chrome_extension/var_api.js", import.meta.url), "utf8");
const { buildVariationalApiScript } = await import(
  `data:text/javascript;base64,${Buffer.from(source).toString("base64")}`
);

const sessions = [
  { open: "2026-10-20T22:00:00Z", close: "2026-10-21T21:00:00Z" },
  { open: "2026-10-21T22:00:00Z", close: "2026-10-22T21:00:00Z" },
  { open: "2026-10-22T22:00:00Z", close: "2026-10-23T21:00:00Z" },
];

async function queryMetadata(json) {
  const requests = [];
  const context = {
    location: { host: "omni.variational.io" },
    localStorage: { length: 0, key: () => null, getItem: () => null },
    performance: { now: () => 0 },
    AbortController: class {
      constructor() {
        this.signal = { aborted: false };
      }
      abort() {
        this.signal.aborted = true;
      }
    },
    setTimeout: () => 1,
    clearTimeout: () => {},
    fetch: async (url, options) => {
      requests.push({ url, options });
      return {
        ok: true,
        status: 200,
        text: async () => JSON.stringify(json),
        headers: { get: () => null },
      };
    },
  };
  const result = await vm.runInNewContext(
    buildVariationalApiScript("MARKET_METADATA", { market: "USOILP" }),
    context,
  );
  return { result: JSON.parse(JSON.stringify(result)), requests };
}

test("reads the live asset-keyed WTI metadata shape and sessions", async () => {
  const { result, requests } = await queryMetadata({
    USOILP: [
      {
        asset: "USOILP",
        name: "Swap on WTI Crude Oil",
        instrument_type: "swap",
        asset_class: "commodity",
        market_status: "open",
        trading_schedule: {
          next_open_at: "2026-10-22T22:00:00Z",
          next_close_at: "2026-10-22T21:00:00Z",
        },
        trading_sessions: sessions,
      },
    ],
  });

  assert.equal(requests[0].url, "https://omni.variational.io/api/metadata/supported_assets?cex_asset=USOILP");
  assert.equal(requests[0].options.credentials, "include");
  assert.deepEqual(result, {
    ok: true,
    market: {
      symbol: "USOILP",
      trading_sessions: sessions,
      trading_schedule: {
        next_open_at: "2026-10-22T22:00:00Z",
        next_close_at: "2026-10-22T21:00:00Z",
      },
      market_status: "open",
    },
    httpStatus: 200,
  });
});

test("accepts legacy symbol listings but rejects identity conflicts", async () => {
  const accepted = await queryMetadata([
    { symbol: "USOILP", trading_sessions: sessions },
  ]);
  assert.equal(accepted.result.ok, true);
  assert.equal(accepted.result.market.symbol, "USOILP");

  const rejected = await queryMetadata({
    USOILP: [{ asset: "USOILP", symbol: "WTI", trading_sessions: sessions }],
  });
  assert.equal(rejected.result.ok, false);
  assert.equal(rejected.result.error, "Market metadata not found.");
});

test("rejects duplicate exact market listings", async () => {
  const { result } = await queryMetadata({
    USOILP: [
      { asset: "USOILP", trading_sessions: sessions },
      { asset: "USOILP", trading_sessions: sessions },
    ],
  });
  assert.equal(result.ok, false);
  assert.equal(result.error, "Market metadata is ambiguous.");
});

test("refuses an expired execution quote before posting an order", async () => {
  const requests = [];
  const context = {
    location: { host: "omni.variational.io" },
    localStorage: { length: 0, key: () => null, getItem: () => null },
    performance: { now: () => 0 },
    AbortController: class {
      constructor() { this.signal = { aborted: false }; }
      abort() { this.signal.aborted = true; }
    },
    setTimeout: () => 1,
    clearTimeout: () => {},
    fetch: async (url, options) => {
      requests.push({ url, options });
      return {
        ok: true, status: 200, text: async () => JSON.stringify({ rfq_id: "unexpected" }),
        headers: { get: () => null },
      };
    },
  };
  const result = await vm.runInNewContext(
    buildVariationalApiScript("ORDER", {
      market: "USOILP", instrumentType: "swap", kind: "commodity",
      side: "BUY", amount: "0.198", reuseQuoteId: "expired-quote",
      quoteTimestamp: "2020-01-01T00:00:00Z", expectedQuotePrice: "100",
      dispatchQuoteAgeLimitMs: 2000, requestTimeoutMs: 1000,
      operationTimeoutMs: 2000,
    }),
    context,
  );
  assert.equal(result.ok, false);
  assert.equal(result.noOrderConfirmed, true);
  assert.equal(result.step, "dispatch_preflight");
  assert.equal(requests.length, 0);
});
