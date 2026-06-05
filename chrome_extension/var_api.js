export function buildVariationalApiScript(action, params) {
  const fn = async (action, o) => {
    const host = location.host || "";
    if (!host.includes("variational.io")) {
      return { ok: false, step: "precheck", error: "Attached tab is not a variational.io page." };
    }

    let address = (o.account && /^0x[0-9a-fA-F]{40}$/.test(o.account)) ? o.account : null;
    if (!address) {
      try {
        for (let i = 0; i < localStorage.length; i += 1) {
          const key = localStorage.key(i) || "";
          const value = localStorage.getItem(key) || "";
          const match = (key + " " + value).match(/0x[0-9a-fA-F]{40}/);
          if (match) {
            address = match[0];
            break;
          }
        }
      } catch (_error) {
        // localStorage can be unavailable in rare browser states; cookies may still authenticate.
      }
    }

    const headers = { "content-type": "application/json" };
    if (address) {
      headers["vr-connected-address"] = address;
    }

    const request = async (method, path, body) => {
      const options = { method, credentials: "include", headers };
      if (body !== undefined) {
        options.body = JSON.stringify(body);
      }
      const response = await fetch("https://omni.variational.io" + path, options);
      const text = await response.text();
      let json = null;
      try {
        json = text ? JSON.parse(text) : null;
      } catch (_error) {
        // Keep raw response text for diagnostics.
      }
      const rateLimitResetMs = response.headers.get("x-rate-limit-resets-in-ms");
      return {
        ok: response.ok,
        status: response.status,
        json,
        text,
        rateLimitResetMs: rateLimitResetMs ? Number(rateLimitResetMs) : null
      };
    };
    const fail = (step, response, extra) => ({
      ok: false,
      step,
      httpStatus: response.status,
      error: response.text || ("HTTP " + response.status),
      addressUsed: Boolean(address),
      ...(extra || {})
    });
    const instrument = {
      underlying: o.market || "BTC",
      instrument_type: "perpetual_future",
      settlement_asset: o.settlementAsset || "USDC",
      funding_interval_s: o.fundingIntervalS || 3600
    };

    if (action === "POSITIONS") {
      const response = await request("GET", "/api/positions", undefined);
      if (!response.ok) {
        return fail("positions", response);
      }
      return { ok: true, positions: response.json, httpStatus: response.status, addressUsed: Boolean(address) };
    }

    if (action === "QUOTE") {
      const response = await request("POST", "/api/quotes/indicative", { instrument, qty: String(o.amount) });
      if (!response.ok || !response.json) {
        return fail("quotes/indicative", response, { rateLimitResetMs: response.rateLimitResetMs });
      }
      const quote = response.json;
      return {
        ok: true,
        quoteId: quote.quote_id,
        bid: quote.bid,
        ask: quote.ask,
        markPrice: quote.mark_price,
        indexPrice: quote.index_price,
        quoteTimestamp: quote.timestamp,
        raw: quote,
        httpStatus: response.status,
        rateLimitResetMs: response.rateLimitResetMs,
        addressUsed: Boolean(address)
      };
    }

    if (action === "ORDER") {
      const side = String(o.side || "").toUpperCase() === "BUY" ? "buy" : "sell";
      let quoteId = o.reuseQuoteId || null;
      let quote = null;
      if (!quoteId) {
        const quoteResponse = await request("POST", "/api/quotes/indicative", { instrument, qty: String(o.amount) });
        if (!quoteResponse.ok || !quoteResponse.json || !quoteResponse.json.quote_id) {
          return fail("quotes/indicative", quoteResponse, { rateLimitResetMs: quoteResponse.rateLimitResetMs });
        }
        quote = quoteResponse.json;
        quoteId = quote.quote_id;
      }
      const orderResponse = await request("POST", "/api/orders/new/market", {
        quote_id: quoteId,
        side,
        max_slippage: o.maxSlippage == null ? 0.005 : o.maxSlippage,
        is_reduce_only: Boolean(o.reduceOnly)
      });
      if (!orderResponse.ok) {
        return fail("orders/new/market", orderResponse, {
          quoteId,
          bid: quote?.bid,
          ask: quote?.ask,
          markPrice: quote?.mark_price,
          rateLimitResetMs: orderResponse.rateLimitResetMs
        });
      }
      return {
        ok: true,
        orderType: "market",
        rfqId: orderResponse.json && orderResponse.json.rfq_id,
        quoteId,
        bid: quote?.bid,
        ask: quote?.ask,
        markPrice: quote?.mark_price,
        quoteTimestamp: quote?.timestamp,
        httpStatus: orderResponse.status,
        addressUsed: Boolean(address)
      };
    }

    return { ok: false, step: "precheck", error: "Unsupported Variational API action: " + action };
  };
  return "(" + fn.toString() + ")(" + JSON.stringify(action) + "," + JSON.stringify(params || {}) + ")";
}
