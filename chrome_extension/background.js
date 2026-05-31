const DEBUGGER_VERSION = "1.3";
const MAX_QUEUE_SIZE = 1000;
const AUTO_RELOAD_COOLDOWN_MS = 5000;

const DEFAULT_CONFIG = {
  wsEndpoint: "ws://127.0.0.1:8766",
  restEndpoint: "ws://127.0.0.1:8767",
  commandEndpoint: "ws://127.0.0.1:8768",
  domainFilter: "variational",
  restAllowlist: [
    "https://omni.variational.io/api/"
  ],
  wsAllowlist: [
    "wss://omni-ws-server.prod.ap-northeast-1.variational.io/events",
    "wss://omni-ws-server.prod.ap-northeast-1.variational.io/portfolio"
  ]
};

const state = {
  active: false,
  attachedTabId: null,
  config: { ...DEFAULT_CONFIG },
  configLoaded: false,
  pendingResponses: new Map(),
  websocketMeta: new Map(),
  lastError: null,
  lastAutoReloadAt: 0
};

class ForwardSocket {
  constructor(label, configKey, options = {}) {
    this.label = label;
    this.configKey = configKey;
    this.onOpen = options.onOpen || null;
    this.onMessage = options.onMessage || null;
    this.ws = null;
    this.status = "disconnected";
    this.queue = [];
    this.retryTimer = null;
  }

  get endpoint() {
    return state.config[this.configKey];
  }

  connect() {
    if (!state.active) {
      return;
    }

    if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
      return;
    }

    const endpoint = this.endpoint;
    if (!endpoint) {
      this.status = "disconnected";
      notifyStatus();
      return;
    }

    this.status = "connecting";
    notifyStatus();

    try {
      const socket = new WebSocket(endpoint);
      this.ws = socket;

      socket.onopen = () => {
        if (this.ws !== socket) {
          return;
        }
        this.status = "connected";
        this.flush();
        if (this.onOpen) {
          this.onOpen(this);
        }
        if (this.configKey === "wsEndpoint") {
          autoReloadAttachedTab("forward receiver connected");
        }
        notifyStatus();
      };

      socket.onmessage = (event) => {
        if (this.ws !== socket || !this.onMessage) {
          return;
        }
        this.onMessage(event.data, this);
      };

      socket.onclose = () => {
        if (this.ws !== socket) {
          return;
        }
        this.ws = null;
        this.status = "disconnected";
        notifyStatus();
        this.scheduleReconnect();
      };

      socket.onerror = () => {
        if (this.ws !== socket) {
          return;
        }
        this.status = "error";
        notifyStatus();
      };
    } catch (error) {
      this.status = "error";
      state.lastError = `${this.label} socket connect failed: ${error.message}`;
      notifyStatus();
      this.scheduleReconnect();
    }
  }

  send(payload) {
    const data = JSON.stringify(payload);
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(data);
      return;
    }

    this.queue.push(data);
    if (this.queue.length > MAX_QUEUE_SIZE) {
      this.queue.shift();
    }
    this.connect();
  }

  flush() {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      return;
    }
    while (this.queue.length > 0) {
      this.ws.send(this.queue.shift());
    }
  }

  scheduleReconnect() {
    if (!state.active || this.retryTimer) {
      return;
    }
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      this.connect();
    }, 1000);
  }

  restart() {
    this.close();
    this.connect();
  }

  close() {
    if (this.retryTimer) {
      clearTimeout(this.retryTimer);
      this.retryTimer = null;
    }
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.status = "disconnected";
    notifyStatus();
  }
}

const wsForwarder = new ForwardSocket("websocket", "wsEndpoint");
const restForwarder = new ForwardSocket("rest", "restEndpoint");
const commandForwarder = new ForwardSocket("command", "commandEndpoint", {
  onOpen: (socket) => socket.send({ type: "REGISTER", role: "extension", timestamp: nowIso() }),
  onMessage: (data) => handleCommandSocketMessage(data)
});

function autoReloadAttachedTab(reason) {
  if (!state.active || state.attachedTabId == null) {
    return;
  }
  const now = Date.now();
  if (now - state.lastAutoReloadAt < AUTO_RELOAD_COOLDOWN_MS) {
    return;
  }
  state.lastAutoReloadAt = now;

  chrome.tabs.reload(state.attachedTabId, {}, () => {
    const err = chrome.runtime.lastError;
    if (err) {
      state.lastError = `Auto reload failed (${reason}): ${err.message}`;
    } else {
      state.lastError = null;
    }
    notifyStatus();
  });
}

async function ensureConfigLoaded() {
  if (state.configLoaded) {
    return;
  }
  const stored = await chrome.storage.local.get("forwarderConfig");
  state.config = sanitizeConfig(stored.forwarderConfig);
  state.configLoaded = true;
}

function sanitizeConfig(incoming = {}) {
  return {
    wsEndpoint: asStringOrDefault(incoming.wsEndpoint, DEFAULT_CONFIG.wsEndpoint),
    restEndpoint: asStringOrDefault(incoming.restEndpoint, DEFAULT_CONFIG.restEndpoint),
    commandEndpoint: asStringOrDefault(incoming.commandEndpoint, DEFAULT_CONFIG.commandEndpoint),
    domainFilter: asStringOrDefault(incoming.domainFilter, DEFAULT_CONFIG.domainFilter),
    restAllowlist: sanitizeRestAllowlist(incoming.restAllowlist),
    wsAllowlist: sanitizeAllowlist(incoming.wsAllowlist, DEFAULT_CONFIG.wsAllowlist)
  };
}

function asStringOrDefault(value, fallback) {
  if (typeof value !== "string") {
    return fallback;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : fallback;
}

function nowIso() {
  return new Date().toISOString();
}

function sanitizeAllowlist(value, fallback) {
  if (!Array.isArray(value)) {
    return [...fallback];
  }
  const cleaned = value
    .filter((item) => typeof item === "string")
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
  if (!cleaned.length) {
    return [...fallback];
  }
  return cleaned;
}

function sanitizeRestAllowlist(value) {
  const cleaned = sanitizeAllowlist(value, DEFAULT_CONFIG.restAllowlist);
  const strict = cleaned.filter((item) => item === DEFAULT_CONFIG.restAllowlist[0]);
  if (!strict.length) {
    return [...DEFAULT_CONFIG.restAllowlist];
  }
  return strict;
}

function matchesDomainFilter(url) {
  const filter = state.config.domainFilter.trim().toLowerCase();
  if (!filter) {
    return true;
  }
  return (url || "").toLowerCase().includes(filter);
}

function normalizeUrlParts(rawUrl) {
  try {
    const parsed = new URL(rawUrl);
    return {
      hostname: parsed.hostname.toLowerCase(),
      pathname: parsed.pathname.toLowerCase(),
      originPath: `${parsed.origin}${parsed.pathname}`,
      full: parsed.toString()
    };
  } catch {
    return {
      hostname: "",
      pathname: "",
      originPath: rawUrl,
      full: rawUrl
    };
  }
}

function isLikelyVariationalRestUrl(url) {
  const target = normalizeUrlParts(url);
  return target.hostname.includes("variational.io") && target.pathname.includes("/api/");
}

function isLikelyVariationalWsUrl(url) {
  const target = normalizeUrlParts(url);
  if (!target.hostname.includes("variational.io")) {
    return false;
  }
  return target.pathname.includes("/events") || target.pathname.includes("/portfolio");
}

function getMatchedRestPattern(url) {
  const patterns = state.config.restAllowlist || [];
  return (
    getMatchedPattern(url, patterns) ||
    (isLikelyVariationalRestUrl(url) ? "auto:variational_rest" : null) ||
    (matchesDomainFilter(url) ? "auto:domain_rest" : null)
  );
}

function getMatchedWsPattern(url) {
  const patterns = state.config.wsAllowlist || [];
  return (
    getMatchedPattern(url, patterns) ||
    (isLikelyVariationalWsUrl(url) ? "auto:variational_ws" : null) ||
    (matchesDomainFilter(url) ? "auto:domain_ws" : null)
  );
}

function getMatchedPattern(url, patterns) {
  if (!patterns.length) {
    return null;
  }

  const target = normalizeUrlParts(url);
  for (const pattern of patterns) {
    const normalizedPattern = normalizeUrlParts(pattern);
    if (target.originPath === normalizedPattern.originPath || target.full.startsWith(pattern)) {
      return pattern;
    }
  }
  return null;
}

async function debuggerAttach(tabId) {
  await new Promise((resolve, reject) => {
    chrome.debugger.attach({ tabId }, DEBUGGER_VERSION, () => {
      const err = chrome.runtime.lastError;
      if (err) {
        reject(new Error(err.message));
        return;
      }
      resolve();
    });
  });
}

async function debuggerDetach(tabId) {
  await new Promise((resolve, reject) => {
    chrome.debugger.detach({ tabId }, () => {
      const err = chrome.runtime.lastError;
      if (err) {
        reject(new Error(err.message));
        return;
      }
      resolve();
    });
  });
}

async function sendDebuggerCommand(tabId, method, params = {}) {
  return new Promise((resolve, reject) => {
    chrome.debugger.sendCommand({ tabId }, method, params, (result) => {
      const err = chrome.runtime.lastError;
      if (err) {
        reject(new Error(err.message));
        return;
      }
      resolve(result || {});
    });
  });
}

async function getActiveTabId() {
  const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
  if (!tabs.length || tabs[0].id == null) {
    throw new Error("No active tab found.");
  }
  return tabs[0].id;
}

async function startForwarding(tabId = null) {
  await ensureConfigLoaded();

  if (state.active) {
    return getStatus();
  }

  const targetTabId = tabId ?? (await getActiveTabId());
  await debuggerAttach(targetTabId);

  try {
    await sendDebuggerCommand(targetTabId, "Network.enable");
  } catch (error) {
    await debuggerDetach(targetTabId);
    throw error;
  }

  state.active = true;
  state.attachedTabId = targetTabId;
  state.lastError = null;
  wsForwarder.connect();
  restForwarder.connect();
  commandForwarder.connect();
  autoReloadAttachedTab("forwarder started");
  notifyStatus();
  return getStatus();
}

async function stopForwarding() {
  const attachedTabId = state.attachedTabId;
  cleanupForwardingState();
  if (attachedTabId != null) {
    try {
      await debuggerDetach(attachedTabId);
    } catch (error) {
      state.lastError = `Debugger detach failed: ${error.message}`;
    }
  }
  notifyStatus();
  return getStatus();
}

function cleanupForwardingState() {
  state.active = false;
  state.pendingResponses.clear();
  state.websocketMeta.clear();
  state.attachedTabId = null;
  state.lastAutoReloadAt = 0;
  wsForwarder.close();
  restForwarder.close();
  commandForwarder.close();
}

async function handleCommandSocketMessage(data) {
  let payload;
  try {
    payload = JSON.parse(data);
  } catch (error) {
    state.lastError = `Command message parse failed: ${error.message}`;
    notifyStatus();
    return;
  }

  const type = String(payload.type || "").toUpperCase();
  if (type === "REGISTER_ACK" || type === "PONG") {
    return;
  }
  if (type === "PAGE_PROBE") {
    await handlePageProbe(payload);
    return;
  }
  if (type === "PLACE_ORDER_DRY_RUN") {
    await handlePlaceOrderDryRun(payload);
    return;
  }
  if (type === "PREPARE_ORDER_DRY_RUN") {
    await handlePrepareOrderDryRun(payload);
    return;
  }
  if (type === "PREPARE_ORDER_KEYBOARD_DRY_RUN") {
    await handlePrepareOrderKeyboardDryRun(payload);
    return;
  }
  if (type === "PREPARE_ORDER_INPUT_SWEEP_DRY_RUN") {
    await handlePrepareOrderInputSweepDryRun(payload);
    return;
  }
  if (type === "PLACE_ORDER") {
    await handlePlaceOrder(payload);
    return;
  }
}

function buildVariationalOrderDomSnapshot(side, amount) {
  const visible = (el) => {
    if (!el) return false;
    const rect = el.getBoundingClientRect();
    const style = window.getComputedStyle(el);
    return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
  };
  const describe = (el) => {
    if (!el) return null;
    const rect = el.getBoundingClientRect();
    return {
      tag: el.tagName,
      type: el.getAttribute('type') || '',
      text: (el.innerText || el.value || el.getAttribute('aria-label') || el.getAttribute('placeholder') || '').trim().slice(0, 120),
      ariaLabel: el.getAttribute('aria-label') || '',
      placeholder: el.getAttribute('placeholder') || '',
      name: el.getAttribute('name') || '',
      id: el.id || '',
      className: String(el.className || '').slice(0, 160),
      disabled: Boolean(el.disabled || el.getAttribute('aria-disabled') === 'true'),
      rect: { x: Math.round(rect.x), y: Math.round(rect.y), width: Math.round(rect.width), height: Math.round(rect.height) }
    };
  };
  const describePanelNode = (el) => {
    const item = describe(el);
    if (!item) return null;
    item.value = typeof el.value === 'string' ? el.value.slice(0, 120) : '';
    item.text = (el.innerText || el.textContent || el.value || el.getAttribute('aria-label') || el.getAttribute('placeholder') || '').trim().replace(/\s+/g, ' ').slice(0, 220);
    return item;
  };
  const all = Array.from(document.querySelectorAll('button, input, textarea, [role="button"], [contenteditable="true"]')).filter(visible);
  const inputs = all.filter((el) => ['INPUT', 'TEXTAREA'].includes(el.tagName) || el.getAttribute('contenteditable') === 'true');
  const buttons = all.filter((el) => el.tagName === 'BUTTON' || el.getAttribute('role') === 'button');
  const sideNeedle = side.toLowerCase() === 'buy' ? ['buy', 'long'] : ['sell', 'short'];
  const sideButton = buttons.find((el) => {
    const haystack = [el.innerText, el.getAttribute('aria-label'), el.id, el.className].join(' ').toLowerCase();
    return sideNeedle.some((needle) => haystack.includes(needle));
  }) || null;
  const textInputs = inputs.filter((el) => (el.getAttribute('type') || '').toLowerCase() !== 'range');
  const mainInput = textInputs
    .map((el) => ({ el, rect: el.getBoundingClientRect() }))
    .sort((a, b) => (b.rect.width - a.rect.width) || (a.rect.y - b.rect.y))[0]?.el || null;
  const submitCandidates = buttons
    .map((el) => ({ el, rect: el.getBoundingClientRect(), item: describePanelNode(el) }))
    .filter(({ rect, item }) => item && rect.x >= 1450 && rect.y >= 180 && rect.y <= 760)
    .sort((a, b) => (a.rect.y - b.rect.y) || (a.rect.x - b.rect.x))
    .map(({ item }) => item)
    .slice(0, 30);
  const panelNodes = Array.from(document.querySelectorAll('button, input, textarea, [role="button"], [contenteditable="true"], [tabindex], div, span'))
    .filter(visible)
    .map(describePanelNode)
    .filter((item) => item && item.rect.x >= 1450 && item.rect.y >= 140 && item.rect.y <= 760)
    .filter((item) => item.text || item.ariaLabel || item.placeholder || item.name || item.id || item.value || ['INPUT', 'TEXTAREA'].includes(item.tag))
    .sort((a, b) => (a.rect.y - b.rect.y) || (a.rect.x - b.rect.x))
    .slice(0, 120);
  return {
    href: location.href,
    title: document.title,
    readyState: document.readyState,
    side,
    amount,
    inputCount: inputs.length,
    buttonCount: buttons.length,
    inputs: inputs.map(describePanelNode).slice(0, 30),
    sideButton: describePanelNode(sideButton),
    mainInput: describePanelNode(mainInput),
    submitCandidates,
    panelNodes,
    buttons: buttons.map(describePanelNode).slice(0, 50)
  };
}

async function handlePrepareOrderDryRun(payload) {
  const requestId = payload.requestId;
  try {
    if (state.attachedTabId == null) {
      throw new Error("No attached tab.");
    }
    const side = String(payload.side || "").toUpperCase();
    const amount = String(payload.amount || "");
    const expression = `(async () => {
      ${buildVariationalOrderDomSnapshot.toString()}
      const snapshotBefore = buildVariationalOrderDomSnapshot(${JSON.stringify("__SIDE__")}, ${JSON.stringify("__AMOUNT__")});
      const input = Array.from(document.querySelectorAll('input, textarea, [contenteditable="true"]'))
        .filter((el) => {
          const rect = el.getBoundingClientRect();
          const style = window.getComputedStyle(el);
          return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none' && (el.getAttribute('type') || '').toLowerCase() !== 'range';
        })
        .sort((a, b) => b.getBoundingClientRect().width - a.getBoundingClientRect().width)[0];
      if (!input) {
        return { ok: false, error: 'No text input candidate found', snapshotBefore };
      }
      input.focus();
      const nativeInputValueSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value')?.set;
      if (nativeInputValueSetter && input instanceof HTMLInputElement) {
        nativeInputValueSetter.call(input, ${JSON.stringify("__AMOUNT__")});
      } else {
        input.value = ${JSON.stringify("__AMOUNT__")};
      }
      input.dispatchEvent(new Event('input', { bubbles: true }));
      input.dispatchEvent(new Event('change', { bubbles: true }));
      await new Promise((resolve) => setTimeout(resolve, 250));
      const snapshotAfter = buildVariationalOrderDomSnapshot(${JSON.stringify("__SIDE__")}, ${JSON.stringify("__AMOUNT__")});
      return { ok: true, action: 'prepared_without_submit', snapshotBefore, snapshotAfter };
    })()`
      .replaceAll('"__SIDE__"', JSON.stringify(side))
      .replaceAll('"__AMOUNT__"', JSON.stringify(amount));
    const result = await sendDebuggerCommand(state.attachedTabId, "Runtime.evaluate", {
      expression,
      returnByValue: true,
      awaitPromise: true,
      userGesture: true
    });
    const value = result.result?.value || null;
    const exceptionText = result.exceptionDetails?.exception?.description || result.exceptionDetails?.text || "";
    commandForwarder.send({
      type: "PREPARE_ORDER_DRY_RUN_RESULT",
      requestId,
      ok: Boolean(value?.ok) && !exceptionText,
      result: value,
      error: exceptionText || value?.error || "",
      timestamp: nowIso()
    });
  } catch (error) {
    commandForwarder.send({
      type: "PREPARE_ORDER_DRY_RUN_RESULT",
      requestId,
      ok: false,
      error: error.message,
      timestamp: nowIso()
    });
  }
}

async function dispatchKey(tabId, params) {
  await sendDebuggerCommand(tabId, "Input.dispatchKeyEvent", params);
}

async function clickPoint(tabId, x, y) {
  await sendDebuggerCommand(tabId, "Input.dispatchMouseEvent", { type: "mousePressed", x, y, button: "left", clickCount: 1 });
  await sendDebuggerCommand(tabId, "Input.dispatchMouseEvent", { type: "mouseReleased", x, y, button: "left", clickCount: 1 });
}

async function clearFocusedInput(tabId) {
  await dispatchKey(tabId, { type: "keyDown", key: "Control", code: "ControlLeft", windowsVirtualKeyCode: 17, nativeVirtualKeyCode: 17, modifiers: 2 });
  await dispatchKey(tabId, { type: "keyDown", key: "a", code: "KeyA", windowsVirtualKeyCode: 65, nativeVirtualKeyCode: 65, modifiers: 2 });
  await dispatchKey(tabId, { type: "keyUp", key: "a", code: "KeyA", windowsVirtualKeyCode: 65, nativeVirtualKeyCode: 65, modifiers: 2 });
  await dispatchKey(tabId, { type: "keyUp", key: "Control", code: "ControlLeft", windowsVirtualKeyCode: 17, nativeVirtualKeyCode: 17 });
  await dispatchKey(tabId, { type: "keyDown", key: "Backspace", code: "Backspace", windowsVirtualKeyCode: 8, nativeVirtualKeyCode: 8 });
  await dispatchKey(tabId, { type: "keyUp", key: "Backspace", code: "Backspace", windowsVirtualKeyCode: 8, nativeVirtualKeyCode: 8 });
}

async function keyboardFillAt(tabId, rect, amount) {
  const x = rect.x + Math.round(rect.width / 2);
  const y = rect.y + Math.round(rect.height / 2);
  await clickPoint(tabId, x, y);
  await clearFocusedInput(tabId);
  await sendDebuggerCommand(tabId, "Input.insertText", { text: amount });
  await new Promise((resolve) => setTimeout(resolve, 400));
  return { x, y };
}

async function handlePrepareOrderKeyboardDryRun(payload) {
  const requestId = payload.requestId;
  try {
    if (state.attachedTabId == null) {
      throw new Error("No attached tab.");
    }
    const side = String(payload.side || "").toUpperCase();
    const amount = String(payload.amount || "");
    const locateExpression = `(() => {
      ${buildVariationalOrderDomSnapshot.toString()}
      return buildVariationalOrderDomSnapshot(${JSON.stringify("__SIDE__")}, ${JSON.stringify("__AMOUNT__")});
    })()`
      .replaceAll('"__SIDE__"', JSON.stringify(side))
      .replaceAll('"__AMOUNT__"', JSON.stringify(amount));
    const beforeResult = await sendDebuggerCommand(state.attachedTabId, "Runtime.evaluate", {
      expression: locateExpression,
      returnByValue: true,
      awaitPromise: false,
      userGesture: true
    });
    const snapshotBefore = beforeResult.result?.value || null;
    const input = snapshotBefore?.mainInput;
    if (!input?.rect) {
      throw new Error("No main input candidate found.");
    }

    const x = input.rect.x + Math.round(input.rect.width / 2);
    const y = input.rect.y + Math.round(input.rect.height / 2);
    await keyboardFillAt(state.attachedTabId, input.rect, amount);
    const afterResult = await sendDebuggerCommand(state.attachedTabId, "Runtime.evaluate", {
      expression: locateExpression,
      returnByValue: true,
      awaitPromise: false,
      userGesture: true
    });
    const snapshotAfter = afterResult.result?.value || null;
    commandForwarder.send({
      type: "PREPARE_ORDER_KEYBOARD_DRY_RUN_RESULT",
      requestId,
      ok: true,
      result: {
        action: "keyboard_prepared_without_submit",
        clickPoint: { x, y },
        snapshotBefore,
        snapshotAfter
      },
      timestamp: nowIso()
    });
  } catch (error) {
    commandForwarder.send({
      type: "PREPARE_ORDER_KEYBOARD_DRY_RUN_RESULT",
      requestId,
      ok: false,
      error: error.message,
      timestamp: nowIso()
    });
  }
}

async function handlePrepareOrderInputSweepDryRun(payload) {
  const requestId = payload.requestId;
  try {
    if (state.attachedTabId == null) {
      throw new Error("No attached tab.");
    }
    const side = String(payload.side || "").toUpperCase();
    const amount = String(payload.amount || "");
    const snapshotExpression = `(() => {
      ${buildVariationalOrderDomSnapshot.toString()}
      return buildVariationalOrderDomSnapshot(${JSON.stringify("__SIDE__")}, ${JSON.stringify("__AMOUNT__")});
    })()`
      .replaceAll('"__SIDE__"', JSON.stringify(side))
      .replaceAll('"__AMOUNT__"', JSON.stringify(amount));
    const beforeResult = await sendDebuggerCommand(state.attachedTabId, "Runtime.evaluate", {
      expression: snapshotExpression,
      returnByValue: true,
      awaitPromise: false,
      userGesture: true
    });
    const snapshotBefore = beforeResult.result?.value || null;
    const candidates = (snapshotBefore?.inputs || [])
      .filter((item) => item.type !== "range" && item.rect && item.rect.width > 0 && item.rect.height > 0)
      .slice(0, 8);
    const attempts = [];
    for (const candidate of candidates) {
      const clickPointResult = await keyboardFillAt(state.attachedTabId, candidate.rect, amount);
      const afterResult = await sendDebuggerCommand(state.attachedTabId, "Runtime.evaluate", {
        expression: snapshotExpression,
        returnByValue: true,
        awaitPromise: false,
        userGesture: true
      });
      const snapshotAfter = afterResult.result?.value || null;
      attempts.push({
        candidate,
        clickPoint: clickPointResult,
        mainInput: snapshotAfter?.mainInput || null,
        submitCandidates: snapshotAfter?.submitCandidates || [],
        panelNodes: snapshotAfter?.panelNodes || []
      });
      const submit = (snapshotAfter?.submitCandidates || []).find((item) => {
        const text = String(item.text || "").toLowerCase();
        return !item.disabled && (text.includes("buy") || text.includes("sell") || text.includes("market") || text.includes("place"));
      });
      if (submit) {
        break;
      }
    }
    commandForwarder.send({
      type: "PREPARE_ORDER_INPUT_SWEEP_DRY_RUN_RESULT",
      requestId,
      ok: true,
      result: {
        action: "input_sweep_without_submit",
        snapshotBefore,
        attempts
      },
      timestamp: nowIso()
    });
  } catch (error) {
    commandForwarder.send({
      type: "PREPARE_ORDER_INPUT_SWEEP_DRY_RUN_RESULT",
      requestId,
      ok: false,
      error: error.message,
      timestamp: nowIso()
    });
  }
}

function findSubmitButton(snapshot, side) {
  const sideText = side.toLowerCase() === "buy" ? "buy" : "sell";
  return (snapshot?.submitCandidates || []).find((item) => {
    const text = String(item.text || "").toLowerCase();
    return !item.disabled && text.includes(sideText) && text.includes("btc") && item.rect;
  }) || null;
}

function parseBtcQuantityFromSnapshot(snapshot) {
  const text = (snapshot?.panelNodes || []).map((item) => item.text || "").join("\n");
  const match = text.match(/Order Quantity\s+([0-9.]+)\s+BTC/i);
  if (!match) {
    return null;
  }
  const value = Number(match[1]);
  return Number.isFinite(value) ? value : null;
}

async function prepareOrderFormWithKeyboard(side, amount) {
  const snapshotExpression = `(() => {
    ${buildVariationalOrderDomSnapshot.toString()}
    return buildVariationalOrderDomSnapshot(${JSON.stringify("__SIDE__")}, ${JSON.stringify("__AMOUNT__")});
  })()`
    .replaceAll('"__SIDE__"', JSON.stringify(side))
    .replaceAll('"__AMOUNT__"', JSON.stringify(amount));
  const beforeResult = await sendDebuggerCommand(state.attachedTabId, "Runtime.evaluate", {
    expression: snapshotExpression,
    returnByValue: true,
    awaitPromise: false,
    userGesture: true
  });
  const snapshotBefore = beforeResult.result?.value || null;
  const candidates = (snapshotBefore?.inputs || [])
    .filter((item) => item.type !== "range" && item.rect && item.rect.width > 0 && item.rect.height > 0)
    .slice(0, 8);
  let selectedAttempt = null;
  for (const candidate of candidates) {
    const inputClickPoint = await keyboardFillAt(state.attachedTabId, candidate.rect, amount);
    const afterResult = await sendDebuggerCommand(state.attachedTabId, "Runtime.evaluate", {
      expression: snapshotExpression,
      returnByValue: true,
      awaitPromise: false,
      userGesture: true
    });
    const snapshotAfter = afterResult.result?.value || null;
    const submitButton = findSubmitButton(snapshotAfter, side);
    selectedAttempt = { candidate, inputClickPoint, snapshotAfter, submitButton };
    if (submitButton) {
      break;
    }
  }
  return { snapshotBefore, selectedAttempt };
}

async function handlePlaceOrder(payload) {
  const requestId = payload.requestId;
  try {
    if (state.attachedTabId == null) {
      throw new Error("No attached tab.");
    }
    const side = String(payload.side || "").toUpperCase();
    const amount = String(payload.amount || "");
    const confirm = Boolean(payload.confirm);
    const expectedMinBtcQty = Number(payload.expectedMinBtcQty || 0);
    const prepared = await prepareOrderFormWithKeyboard(side, amount);
    const submitButton = prepared.selectedAttempt?.submitButton || null;
    if (!submitButton) {
      commandForwarder.send({
        type: "ORDER_RESULT",
        requestId,
        ok: false,
        error: "No enabled submit button found after preparing order form.",
        result: prepared,
        timestamp: nowIso()
      });
      return;
    }
    const orderQuantityBtc = parseBtcQuantityFromSnapshot(prepared.selectedAttempt?.snapshotAfter);
    if (expectedMinBtcQty > 0 && (orderQuantityBtc == null || orderQuantityBtc < expectedMinBtcQty)) {
      commandForwarder.send({
        type: "ORDER_RESULT",
        requestId,
        ok: false,
        error: `Order Quantity ${orderQuantityBtc ?? 'unknown'} BTC is below expected minimum ${expectedMinBtcQty} BTC. Not clicking.`,
        result: { ...prepared, submitButton, orderQuantityBtc, clicked: false },
        timestamp: nowIso()
      });
      return;
    }

    const submitClickPoint = {
      x: submitButton.rect.x + Math.round(submitButton.rect.width / 2),
      y: submitButton.rect.y + Math.round(submitButton.rect.height / 2)
    };
    if (!confirm) {
      commandForwarder.send({
        type: "ORDER_RESULT",
        requestId,
        ok: false,
        error: "PLACE_ORDER requires confirm=true. Prepared form but did not click submit.",
        result: { ...prepared, submitButton, orderQuantityBtc, clickPoint: submitClickPoint, clicked: false },
        timestamp: nowIso()
      });
      return;
    }

    await clickPoint(state.attachedTabId, submitClickPoint.x, submitClickPoint.y);
    commandForwarder.send({
      type: "ORDER_RESULT",
      requestId,
      ok: true,
      result: {
        ...prepared,
        submitButton,
        orderQuantityBtc,
        clickPoint: submitClickPoint,
        clicked: true,
        clickedAt: nowIso()
      },
      timestamp: nowIso()
    });
  } catch (error) {
    commandForwarder.send({
      type: "ORDER_RESULT",
      requestId,
      ok: false,
      error: error.message,
      timestamp: nowIso()
    });
  }
}

async function handlePlaceOrderDryRun(payload) {
  const requestId = payload.requestId;
  try {
    if (state.attachedTabId == null) {
      throw new Error("No attached tab.");
    }
    const side = String(payload.side || "").toUpperCase();
    const amount = String(payload.amount || "");
    const expression = `(() => {
      const visible = (el) => {
        if (!el) return false;
        const rect = el.getBoundingClientRect();
        const style = window.getComputedStyle(el);
        return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
      };
      const describe = (el) => {
        if (!el) return null;
        const rect = el.getBoundingClientRect();
        return {
          tag: el.tagName,
          type: el.getAttribute('type') || '',
          text: (el.innerText || el.value || el.getAttribute('aria-label') || el.getAttribute('placeholder') || '').trim().slice(0, 120),
          ariaLabel: el.getAttribute('aria-label') || '',
          placeholder: el.getAttribute('placeholder') || '',
          name: el.getAttribute('name') || '',
          id: el.id || '',
          className: String(el.className || '').slice(0, 160),
          disabled: Boolean(el.disabled || el.getAttribute('aria-disabled') === 'true'),
          rect: { x: Math.round(rect.x), y: Math.round(rect.y), width: Math.round(rect.width), height: Math.round(rect.height) }
        };
      };
      const describePanelNode = (el) => {
        const item = describe(el);
        if (!item) return null;
        item.value = typeof el.value === 'string' ? el.value.slice(0, 120) : '';
        item.text = (el.innerText || el.textContent || el.value || el.getAttribute('aria-label') || el.getAttribute('placeholder') || '').trim().replace(/\s+/g, ' ').slice(0, 220);
        return item;
      };
      const nearestCommonAncestor = (a, b) => {
        if (!a || !b) return null;
        const seen = new Set();
        let cur = a;
        while (cur) {
          seen.add(cur);
          cur = cur.parentElement;
        }
        cur = b;
        while (cur) {
          if (seen.has(cur)) return cur;
          cur = cur.parentElement;
        }
        return null;
      };
      const ancestorsOf = (el) => {
        const out = [];
        let cur = el;
        while (cur && out.length < 12) {
          const item = describePanelNode(cur);
          if (item) {
            item.childElementCount = cur.childElementCount || 0;
            item.innerTextPreview = (cur.innerText || cur.textContent || '').trim().replace(/\s+/g, ' ').slice(0, 500);
            out.push(item);
          }
          cur = cur.parentElement;
        }
        return out;
      };
      const centerOf = (item) => ({
        x: item.rect.x + Math.round(item.rect.width / 2),
        y: item.rect.y + Math.round(item.rect.height / 2)
      });
      const all = Array.from(document.querySelectorAll('button, input, textarea, [role="button"], [contenteditable="true"]')).filter(visible);
      const inputs = all.filter((el) => ['INPUT', 'TEXTAREA'].includes(el.tagName) || el.getAttribute('contenteditable') === 'true').map(describe);
      const buttons = all.filter((el) => el.tagName === 'BUTTON' || el.getAttribute('role') === 'button').map(describe);
      const sideNeedle = ${JSON.stringify("__SIDE__")}.toLowerCase() === 'buy' ? ['buy', 'long'] : ['sell', 'short'];
      const sideButtons = buttons.filter((item) => {
        const haystack = [item.text, item.ariaLabel, item.id, item.className].join(' ').toLowerCase();
        return sideNeedle.some((needle) => haystack.includes(needle));
      });
      const buyButton = all.find((el) => (el.innerText || '').trim().toLowerCase() === 'buy');
      const sellButton = all.find((el) => (el.innerText || '').trim().toLowerCase() === 'sell');
      const panelRoot = nearestCommonAncestor(buyButton, sellButton);
      const buyDescription = describe(buyButton);
      const sellDescription = describe(sellButton);
      const nearRect = buyDescription && sellDescription
        ? {
            left: Math.min(buyDescription.rect.x, sellDescription.rect.x) - 260,
            right: Math.max(buyDescription.rect.x + buyDescription.rect.width, sellDescription.rect.x + sellDescription.rect.width) + 260,
            top: Math.min(buyDescription.rect.y, sellDescription.rect.y) - 420,
            bottom: Math.max(buyDescription.rect.y + buyDescription.rect.height, sellDescription.rect.y + sellDescription.rect.height) + 160
          }
        : null;
      const nearNodes = nearRect
        ? Array.from(document.querySelectorAll('button, input, textarea, [role="button"], [contenteditable="true"], [tabindex], div, span'))
            .filter(visible)
            .map(describePanelNode)
            .filter((item) => item && item.rect.x >= nearRect.left && item.rect.x <= nearRect.right && item.rect.y >= nearRect.top && item.rect.y <= nearRect.bottom)
            .filter((item) => item.text || item.ariaLabel || item.placeholder || item.name || item.id || item.value || ['INPUT', 'TEXTAREA'].includes(item.tag))
            .sort((a, b) => (a.rect.y - b.rect.y) || (a.rect.x - b.rect.x))
            .slice(0, 220)
        : [];
      const pointSamples = buyDescription && sellDescription
        ? [-360, -300, -240, -180, -120, -60, 0, 60, 120].map((dy) => {
            const center = centerOf(buyDescription);
            const el = document.elementFromPoint(center.x, center.y + dy);
            const item = describePanelNode(el);
            return { dx: 0, dy, element: item };
          })
        : [];
      const panelNodes = panelRoot
        ? Array.from(panelRoot.querySelectorAll('button, input, textarea, [role="button"], [contenteditable="true"], [tabindex], div, span'))
            .filter(visible)
            .map(describePanelNode)
            .filter((item) => item && (item.text || item.ariaLabel || item.placeholder || item.name || item.id || item.value))
            .slice(0, 160)
        : [];
      const panelText = panelRoot ? (panelRoot.innerText || panelRoot.textContent || '').trim().replace(/\s+/g, ' ').slice(0, 3000) : '';
      return {
        href: location.href,
        title: document.title,
        readyState: document.readyState,
        side: ${JSON.stringify("__SIDE__")},
        amount: ${JSON.stringify("__AMOUNT__")},
        inputCount: inputs.length,
        buttonCount: buttons.length,
        inputs: inputs.slice(0, 30),
        sideButtons: sideButtons.slice(0, 20),
        panelText,
        panelNodes,
        buyAncestors: ancestorsOf(buyButton),
        sellAncestors: ancestorsOf(sellButton),
        nearNodes,
        pointSamples,
        buttons: buttons.slice(0, 40)
      };
    })()`
      .replaceAll('"__SIDE__"', JSON.stringify(side))
      .replaceAll('"__AMOUNT__"', JSON.stringify(amount));
    const result = await sendDebuggerCommand(state.attachedTabId, "Runtime.evaluate", {
      expression,
      returnByValue: true,
      awaitPromise: false,
      userGesture: false
    });
    commandForwarder.send({
      type: "PLACE_ORDER_DRY_RUN_RESULT",
      requestId,
      ok: true,
      result: result.result?.value || null,
      timestamp: nowIso()
    });
  } catch (error) {
    commandForwarder.send({
      type: "PLACE_ORDER_DRY_RUN_RESULT",
      requestId,
      ok: false,
      error: error.message,
      timestamp: nowIso()
    });
  }
}

async function handlePageProbe(payload) {
  const requestId = payload.requestId;
  try {
    if (state.attachedTabId == null) {
      throw new Error("No attached tab.");
    }
    const result = await sendDebuggerCommand(state.attachedTabId, "Runtime.evaluate", {
      expression: "({ title: document.title, href: location.href, readyState: document.readyState })",
      returnByValue: true,
      awaitPromise: false,
      userGesture: false
    });
    commandForwarder.send({
      type: "PAGE_PROBE_RESULT",
      requestId,
      ok: true,
      result: result.result?.value || null,
      timestamp: nowIso()
    });
  } catch (error) {
    commandForwarder.send({
      type: "PAGE_PROBE_RESULT",
      requestId,
      ok: false,
      error: error.message,
      timestamp: nowIso()
    });
  }
}

function getStatus() {
  return {
    active: state.active,
    attachedTabId: state.attachedTabId,
    config: state.config,
    sockets: {
      websocket: wsForwarder.status,
      rest: restForwarder.status,
      command: commandForwarder.status
    },
    lastError: state.lastError
  };
}

function notifyStatus() {
  chrome.runtime.sendMessage({ event: "status", status: getStatus() }).catch(() => {
    // No listeners (popup closed), safe to ignore.
  });
}

function trackResponse(params) {
  if (!params?.response?.url || !matchesDomainFilter(params.response.url)) {
    return;
  }
  if (params.type !== "Fetch" && params.type !== "XHR") {
    return;
  }

  const matchedPattern = getMatchedRestPattern(params.response.url);
  if (!matchedPattern) {
    return;
  }

  state.pendingResponses.set(params.requestId, {
    requestId: params.requestId,
    url: params.response.url,
    status: params.response.status,
    statusText: params.response.statusText,
    mimeType: params.response.mimeType,
    headers: params.response.headers,
    type: params.type,
    matchedPattern,
    capturedAt: nowIso()
  });
}

async function forwardResponseBody(requestId, encodedDataLength) {
  const meta = state.pendingResponses.get(requestId);
  if (!meta || state.attachedTabId == null) {
    return;
  }
  state.pendingResponses.delete(requestId);

  try {
    const result = await sendDebuggerCommand(state.attachedTabId, "Network.getResponseBody", { requestId });
    restForwarder.send({
      kind: "rest_response",
      requestId,
      timestamp: nowIso(),
      encodedDataLength,
      ...meta,
      body: result.body ?? "",
      base64Encoded: Boolean(result.base64Encoded)
    });
  } catch (error) {
    restForwarder.send({
      kind: "rest_response_error",
      requestId,
      timestamp: nowIso(),
      ...meta,
      error: error.message
    });
  }
}

function forwardWebSocketFrame(direction, params) {
  const meta = state.websocketMeta.get(params.requestId);
  if (!meta) {
    return;
  }

  wsForwarder.send({
    kind: "ws_frame",
    direction,
    requestId: params.requestId,
    url: meta.url,
    matchedPattern: meta.matchedPattern || "",
    timestamp: nowIso(),
    opcode: params.response?.opcode,
    mask: params.response?.mask,
    payloadData: params.response?.payloadData ?? ""
  });
}

async function handleDebuggerEvent(source, method, params) {
  if (!state.active || source.tabId !== state.attachedTabId) {
    return;
  }

  if (method === "Network.responseReceived") {
    trackResponse(params);
    return;
  }

  if (method === "Network.loadingFinished") {
    await forwardResponseBody(params.requestId, params.encodedDataLength);
    return;
  }

  if (method === "Network.loadingFailed") {
    state.pendingResponses.delete(params.requestId);
    return;
  }

  if (method === "Network.webSocketCreated") {
    const matchedPattern = getMatchedWsPattern(params.url);
    if (matchesDomainFilter(params.url) && matchedPattern) {
      state.websocketMeta.set(params.requestId, {
        url: params.url,
        matchedPattern,
        createdAt: nowIso()
      });
    }
    return;
  }

  if (method === "Network.webSocketClosed") {
    const meta = state.websocketMeta.get(params.requestId);
    if (!meta) {
      return;
    }
    wsForwarder.send({
      kind: "ws_closed",
      requestId: params.requestId,
      url: meta.url,
      matchedPattern: meta.matchedPattern || "",
      timestamp: nowIso()
    });
    state.websocketMeta.delete(params.requestId);
    return;
  }

  if (method === "Network.webSocketFrameReceived") {
    forwardWebSocketFrame("received", params);
    return;
  }

  if (method === "Network.webSocketFrameSent") {
    forwardWebSocketFrame("sent", params);
    return;
  }

  if (method === "Network.webSocketFrameError") {
    const meta = state.websocketMeta.get(params.requestId);
    if (!meta) {
      return;
    }
    wsForwarder.send({
      kind: "ws_frame_error",
      requestId: params.requestId,
      url: meta.url,
      matchedPattern: meta.matchedPattern || "",
      timestamp: nowIso(),
      errorMessage: params.errorMessage || "Unknown WebSocket frame error"
    });
  }
}

chrome.debugger.onEvent.addListener((source, method, params) => {
  handleDebuggerEvent(source, method, params).catch((error) => {
    state.lastError = `CDP event handling failed: ${error.message}`;
    notifyStatus();
  });
});

chrome.debugger.onDetach.addListener((source, reason) => {
  if (source.tabId !== state.attachedTabId) {
    return;
  }
  state.lastError = `Debugger detached: ${reason}`;
  cleanupForwardingState();
  notifyStatus();
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  (async () => {
    await ensureConfigLoaded();

    if (message.action === "getStatus") {
      return { ok: true, status: getStatus() };
    }

    if (message.action === "updateConfig") {
      state.config = sanitizeConfig(message.config);
      await chrome.storage.local.set({ forwarderConfig: state.config });
      if (state.active) {
        wsForwarder.restart();
        restForwarder.restart();
      }
      notifyStatus();
      return { ok: true, status: getStatus() };
    }

    if (message.action === "start") {
      const status = await startForwarding(message.tabId ?? null);
      return { ok: true, status };
    }

    if (message.action === "stop") {
      const status = await stopForwarding();
      return { ok: true, status };
    }

    return { ok: false, error: `Unknown action: ${message.action}` };
  })()
    .then((response) => sendResponse(response))
    .catch((error) => sendResponse({ ok: false, error: error.message }));

  return true;
});

chrome.runtime.onInstalled.addListener(() => {
  ensureConfigLoaded().catch(() => {
    // Ignore config load errors during install.
  });
});
