type HeartbeatRow = {
  id: string;
  updated_at: string;
  payload: Record<string, unknown>;
};

type MonitorStateRow = {
  id: string;
  state: "up" | "degraded" | "down";
  reason: string | null;
  reason_key: string | null;
  metadata: Record<string, unknown>;
};

const SUPABASE_URL = Deno.env.get("SUPABASE_URL") ?? mustEnv("PROJECT_URL");
const SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? mustEnv("SERVICE_ROLE_KEY");
const TELEGRAM_BOT_TOKEN = mustEnv("TELEGRAM_BOT_TOKEN");
const TELEGRAM_CHAT_ID = mustEnv("TELEGRAM_CHAT_ID");

const HEARTBEAT_ID = Deno.env.get("HEARTBEAT_ID") ?? "scalper-alpine";
const MONITOR_ID = Deno.env.get("MONITOR_ID") ?? HEARTBEAT_ID;
const MONITOR_SECRET = Deno.env.get("MONITOR_SECRET");
const STALE_SECONDS = numberEnv("HEARTBEAT_STALE_SECONDS", 300);
const RAM_CRITICAL_PERCENT = numberEnv("RAM_CRITICAL_PERCENT", 85);
const DISK_CRITICAL_PERCENT = numberEnv("DISK_CRITICAL_PERCENT", 90);
const TEMP_CRITICAL_C = numberEnv("TEMP_CRITICAL_C", 80);
const NO_EVENTS_SECONDS = numberEnv("NO_EVENTS_SECONDS", 300);

Deno.serve(async (request) => {
  try {
    if (MONITOR_SECRET && request.headers.get("x-monitor-secret") !== MONITOR_SECRET) {
      return json({ ok: false, error: "unauthorized" }, 401);
    }

    const now = new Date();
    const previous = await readMonitorState();
    const heartbeat = await readHeartbeat();
    const evaluation = evaluate(heartbeat, previous, now);

    const shouldNotify =
      !previous ||
      previous.state !== evaluation.state ||
      previous.reason_key !== evaluation.reasonKey;

    if (shouldNotify) {
      await sendTelegram(formatMessage(evaluation, heartbeat, now));
    }

    await writeMonitorState(evaluation, previous, now, shouldNotify);

    return json({
      ok: true,
      state: evaluation.state,
      reason: evaluation.reason,
      reason_key: evaluation.reasonKey,
      notified: shouldNotify,
    });
  } catch (error) {
    return json({
      ok: false,
      error: error instanceof Error ? error.message : String(error),
    }, 500);
  }
});

function mustEnv(name: string): string {
  const value = Deno.env.get(name);
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function numberEnv(name: string, fallback: number): number {
  const raw = Deno.env.get(name);
  if (!raw) return fallback;
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : fallback;
}

async function readHeartbeat(): Promise<HeartbeatRow | null> {
  const rows = await rest<HeartbeatRow[]>(
    `/rest/v1/server_heartbeat?id=eq.${encodeURIComponent(HEARTBEAT_ID)}&select=id,updated_at,payload&limit=1`,
  );
  return rows[0] ?? null;
}

async function readMonitorState(): Promise<MonitorStateRow | null> {
  const rows = await rest<MonitorStateRow[]>(
    `/rest/v1/monitor_state?id=eq.${encodeURIComponent(MONITOR_ID)}&select=id,state,reason,reason_key,metadata&limit=1`,
  );
  return rows[0] ?? null;
}

async function writeMonitorState(
  evaluation: Evaluation,
  previous: MonitorStateRow | null,
  now: Date,
  notified: boolean,
): Promise<void> {
  const metadata = {
    ...(previous?.metadata ?? {}),
    ...evaluation.metadata,
  };

  await rest("/rest/v1/monitor_state", {
    method: "POST",
    headers: { Prefer: "resolution=merge-duplicates" },
    body: JSON.stringify({
      id: MONITOR_ID,
      state: evaluation.state,
      reason: evaluation.reason,
      reason_key: evaluation.reasonKey,
      metadata,
      notified_at: notified ? now.toISOString() : undefined,
      updated_at: now.toISOString(),
    }),
  });
}

type Evaluation = {
  state: "up" | "degraded" | "down";
  reason: string;
  reasonKey: string;
  metadata: Record<string, unknown>;
};

function evaluate(
  heartbeat: HeartbeatRow | null,
  previous: MonitorStateRow | null,
  now: Date,
): Evaluation {
  const reasons: string[] = [];
  const metadata: Record<string, unknown> = {};

  if (!heartbeat) {
    return {
      state: "down",
      reason: "heartbeat missing",
      reasonKey: "heartbeat_missing",
      metadata,
    };
  }

  const payload = heartbeat.payload as Record<string, unknown>;
  const ageSeconds = Math.max(0, Math.floor((now.getTime() - new Date(heartbeat.updated_at).getTime()) / 1000));
  metadata.heartbeat_updated_at = heartbeat.updated_at;
  metadata.heartbeat_age_seconds = ageSeconds;

  if (ageSeconds > STALE_SECONDS) {
    return {
      state: "down",
      reason: `heartbeat stale: ${ageSeconds}s`,
      reasonKey: "heartbeat_stale",
      metadata,
    };
  }

  const server = objectAt(payload, "server");
  const scalper = objectAt(payload, "scalper");
  const health = objectAt(scalper, "health");

  if (stringAt(scalper, "service") !== "started") reasons.push("scalper service stopped");
  if (stringAt(scalper, "http") !== "ok") reasons.push("scalper health HTTP down");

  const healthState = stringAt(health, "state");
  if (healthState && healthState !== "PAPER_READY") reasons.push(`scalper state ${healthState}`);

  const storage = objectAt(health, "storage");
  const flushErrors = numberAt(storage, "flush_errors");
  if (flushErrors > 0) reasons.push(`storage flush_errors ${flushErrors}`);

  const orderbook = objectAt(health, "orderbook");
  if (booleanAt(orderbook, "synced") === false) reasons.push("orderbook desynced");

  const memory = objectAt(server, "memory");
  const ramPercent = numberAt(memory, "used_percent");
  if (ramPercent >= RAM_CRITICAL_PERCENT) reasons.push(`RAM ${ramPercent}%`);

  const disk = objectAt(server, "disk");
  const usbDisk = numberAt(disk, "usb_used_percent");
  const scalperDisk = numberAt(disk, "scalper_used_percent");
  if (usbDisk >= DISK_CRITICAL_PERCENT) reasons.push(`USB disk ${usbDisk}%`);
  if (scalperDisk >= DISK_CRITICAL_PERCENT) reasons.push(`scalper disk ${scalperDisk}%`);

  const cpu = objectAt(server, "cpu");
  const tempC = numberAt(cpu, "temp_c");
  if (tempC >= TEMP_CRITICAL_C) reasons.push(`CPU temp ${tempC}C`);

  const paper = objectAt(health, "paper");
  if (booleanAt(paper, "kill_switch") === true) reasons.push("paper kill switch active");

  const marketEvents = numberAt(health, "market_events_seen");
  const previousEvents = numberFromUnknown(previous?.metadata?.market_events_seen);
  const previousAdvancedAt = stringFromUnknown(previous?.metadata?.market_events_advanced_at);
  let marketEventsAdvancedAt = previousAdvancedAt ?? now.toISOString();

  if (marketEvents > previousEvents) {
    marketEventsAdvancedAt = now.toISOString();
  } else {
    const quietSeconds = Math.floor((now.getTime() - new Date(marketEventsAdvancedAt).getTime()) / 1000);
    if (quietSeconds > NO_EVENTS_SECONDS) reasons.push(`no market events for ${quietSeconds}s`);
  }

  metadata.market_events_seen = marketEvents;
  metadata.market_events_advanced_at = marketEventsAdvancedAt;

  if (reasons.length === 0) {
    return {
      state: "up",
      reason: "healthy",
      reasonKey: "healthy",
      metadata,
    };
  }

  return {
    state: "degraded",
    reason: reasons.join("; "),
    reasonKey: reasons.map(reasonKey).join("|"),
    metadata,
  };
}

function formatMessage(evaluation: Evaluation, heartbeat: HeartbeatRow | null, now: Date): string {
  const payload = (heartbeat?.payload ?? {}) as Record<string, unknown>;
  const server = objectAt(payload, "server");
  const scalper = objectAt(payload, "scalper");
  const health = objectAt(scalper, "health");
  const paper = objectAt(health, "paper");
  const storage = objectAt(health, "storage");

  return [
    `scalper monitor: ${evaluation.state.toUpperCase()}`,
    `reason: ${evaluation.reason}`,
    `time: ${now.toISOString()}`,
    `heartbeat: ${heartbeat?.updated_at ?? "missing"}`,
    `events: ${numberAt(health, "market_events_seen")}`,
    `pnl_usdt: ${numberAt(paper, "realized_pnl_usdt")}`,
    `open_positions: ${numberAt(paper, "open_positions")}`,
    `ram: ${numberAt(objectAt(server, "memory"), "used_percent")}%`,
    `disk_scalper: ${numberAt(objectAt(server, "disk"), "scalper_used_percent")}%`,
    `temp_c: ${numberAt(objectAt(server, "cpu"), "temp_c")}`,
    `flush_errors: ${numberAt(storage, "flush_errors")}`,
  ].join("\n");
}

async function sendTelegram(text: string): Promise<void> {
  const response = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      chat_id: TELEGRAM_CHAT_ID,
      text,
      disable_web_page_preview: "true",
    }),
  });

  if (!response.ok) {
    throw new Error(`telegram send failed: ${response.status} ${await response.text()}`);
  }
}

async function rest<T = unknown>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  headers.set("apikey", SERVICE_ROLE_KEY);
  headers.set("Authorization", `Bearer ${SERVICE_ROLE_KEY}`);
  headers.set("Content-Type", "application/json");

  const response = await fetch(`${SUPABASE_URL}${path}`, {
    ...init,
    headers,
  });

  if (!response.ok) {
    throw new Error(`supabase REST failed: ${response.status} ${await response.text()}`);
  }

  if (response.status === 204) return undefined as T;
  const text = await response.text();
  if (!text) return undefined as T;
  return JSON.parse(text) as T;
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function objectAt(value: unknown, key: string): Record<string, unknown> {
  if (!value || typeof value !== "object") return {};
  const next = (value as Record<string, unknown>)[key];
  return next && typeof next === "object" ? next as Record<string, unknown> : {};
}

function stringAt(value: unknown, key: string): string {
  if (!value || typeof value !== "object") return "";
  const next = (value as Record<string, unknown>)[key];
  return typeof next === "string" ? next : "";
}

function booleanAt(value: unknown, key: string): boolean | null {
  if (!value || typeof value !== "object") return null;
  const next = (value as Record<string, unknown>)[key];
  return typeof next === "boolean" ? next : null;
}

function numberAt(value: unknown, key: string): number {
  if (!value || typeof value !== "object") return 0;
  return numberFromUnknown((value as Record<string, unknown>)[key]);
}

function numberFromUnknown(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function stringFromUnknown(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function reasonKey(reason: string): string {
  return reason.toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
}
