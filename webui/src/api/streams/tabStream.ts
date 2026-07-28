// One upstream server-stream per stream name, held by whichever Backrest tab
// the user touched most recently. Backrest is usually served over http://
// (h2c), where browsers use HTTP/1.1 and cap ~6 connections/origin across all
// tabs, so a stream per tab exhausts the pool — and a full pool hangs rather
// than errors.
//
// Rules:
//   1. A tab may stream while it is visible (brief hides are forgiven for
//      hiddenGraceMs) and holds the newest claim among Backrest tabs.
//   2. A tab claims by broadcasting {ts, id} when it gains focus or becomes
//      visible. The newest claim wins, ties broken by id, so all tabs agree
//      on the winner. A closing tab broadcasts a release so a visible
//      survivor can reclaim without waiting for a click.
//   3. Every connect attempt starts by telling subscribers to reload from the
//      API, covering any deltas missed since the previous connection. Reloads
//      are idempotent and the reconnect backoff bounds their rate.
//
// The channel carries only claim/release messages, never data. If
// BroadcastChannel is missing or a message is lost, tabs merely fail to stand
// down: extra connections, never wrong data. A tab that stood down keeps its
// last-loaded state and catches up when it next claims.

/** A tab hidden at least this long stops streaming until it's next visible. */
export const HIDDEN_GRACE_MS = 120_000;

const DEFAULT_BACKOFF_MS = 5_000;

export interface StreamSubscriber<T> {
  onMessage(msg: T): void;
  /** Reload full state from the API: fired on subscribe and at the start of
   *  every connect attempt (rule 3 above). */
  onConnectOrResync(): void;
}

export interface TabStreamOpts<T> {
  /** Names the claim channel; shared by all tabs streaming the same thing. */
  name: string;
  connect: (signal: AbortSignal) => AsyncIterable<T>;
  backoffMs?: number;
  /** Overridable for tests. */
  hiddenGraceMs?: number;
}

export interface TabStream<T> {
  /** First subscribe starts the stream, last unsubscribe tears it down. */
  subscribe(sub: StreamSubscriber<T>): () => void;
}

export function createTabStream<T>(opts: TabStreamOpts<T>): TabStream<T> {
  return new TabStreamImpl(opts);
}

interface Claim {
  ts: number;
  id: string;
}

type ClaimMsg =
  | { t: "claim"; ts: number; id: string }
  | { t: "release"; id: string };

// Tabs share a clock (same machine), so timestamps are comparable; the id
// tiebreak only matters for same-millisecond claims and just has to be a rule
// every tab applies identically.
const newerClaim = (a: Claim, b: Claim) =>
  a.ts !== b.ts ? a.ts > b.ts : a.id > b.id;

/** Tracks rule 1: is this tab visible and holding the newest claim? */
class Eligibility {
  private readonly id = Math.random().toString(36).slice(2);
  /** Newest claim seen from any tab, ours included. */
  private latest: Claim = { ts: 0, id: this.id };
  private visible = true;
  private hiddenTimer: ReturnType<typeof setTimeout> | null = null;
  private channel: BroadcastChannel | null = null;

  /** Single consumer (the stream loop); fired on any possible change. */
  onChange: (() => void) | null = null;

  constructor(
    name: string,
    private readonly hiddenGraceMs: number,
  ) {
    if (typeof document === "undefined") return; // non-browser: always eligible
    this.visible = !document.hidden;
    document.addEventListener("visibilitychange", this.onVisibilityChange);
    window.addEventListener("focus", this.claim);
    window.addEventListener("pageshow", this.claimIfVisible);
    window.addEventListener("pagehide", this.release);
    if (typeof BroadcastChannel !== "undefined") {
      try {
        this.channel = new BroadcastChannel(name);
        this.channel.onmessage = (e) => this.onBusMessage(e.data as ClaimMsg);
      } catch (err) {
        console.warn(`[tabStream:${name}] BroadcastChannel unusable`, err);
      }
    }
    this.claimIfVisible();
  }

  get eligible(): boolean {
    return this.visible && this.latest.id === this.id;
  }

  stop() {
    if (typeof document !== "undefined") {
      document.removeEventListener("visibilitychange", this.onVisibilityChange);
      window.removeEventListener("focus", this.claim);
      window.removeEventListener("pageshow", this.claimIfVisible);
      window.removeEventListener("pagehide", this.release);
    }
    if (this.hiddenTimer !== null) clearTimeout(this.hiddenTimer);
    this.release();
    if (this.channel) {
      this.channel.onmessage = null;
      this.channel.close();
      this.channel = null;
    }
    this.onChange = null;
  }

  /** Resolves on the next possible eligibility change, or when signal aborts. */
  changed(signal: AbortSignal): Promise<void> {
    return new Promise((resolve) => {
      if (signal.aborted) return resolve();
      const done = () => {
        this.onChange = null;
        signal.removeEventListener("abort", done);
        resolve();
      };
      this.onChange = done;
      signal.addEventListener("abort", done, { once: true });
    });
  }

  private readonly claim = () => {
    const mine: Claim = { ts: Date.now(), id: this.id };
    if (newerClaim(mine, this.latest)) this.latest = mine;
    this.channel?.postMessage({ t: "claim", ...mine } satisfies ClaimMsg);
    this.onChange?.();
  };

  private readonly claimIfVisible = () => {
    if (this.visible) this.claim();
  };

  private readonly release = () => {
    this.channel?.postMessage({ t: "release", id: this.id } satisfies ClaimMsg);
  };

  private onBusMessage(msg: ClaimMsg) {
    if (msg.t === "claim") {
      const theirs: Claim = { ts: msg.ts, id: msg.id };
      if (newerClaim(theirs, this.latest)) {
        this.latest = theirs;
        this.onChange?.();
      }
    } else if (msg.id === this.latest.id) {
      // The streaming tab went away: void its claim, then reclaim if visible.
      // Concurrent reclaims all broadcast and newest-claim-wins settles on one.
      this.latest = { ts: 0, id: this.id };
      this.claimIfVisible();
    }
  }

  private readonly onVisibilityChange = () => {
    if (document.hidden) {
      this.hiddenTimer ??= setTimeout(() => {
        this.hiddenTimer = null;
        this.visible = false;
        this.onChange?.();
      }, this.hiddenGraceMs);
    } else {
      if (this.hiddenTimer !== null) clearTimeout(this.hiddenTimer);
      this.hiddenTimer = null;
      this.visible = true;
      this.claim();
    }
  };
}

class TabStreamImpl<T> implements TabStream<T> {
  private readonly backoffMs: number;
  private readonly hiddenGraceMs: number;
  private readonly subscribers = new Set<StreamSubscriber<T>>();

  // Non-null while running; aborting tears down the loop and eligibility.
  private run: AbortController | null = null;
  private eligibility: Eligibility | null = null;

  constructor(private readonly opts: TabStreamOpts<T>) {
    this.backoffMs = opts.backoffMs ?? DEFAULT_BACKOFF_MS;
    this.hiddenGraceMs = opts.hiddenGraceMs ?? HIDDEN_GRACE_MS;
  }

  subscribe(sub: StreamSubscriber<T>): () => void {
    this.subscribers.add(sub);
    if (!this.run) {
      this.run = new AbortController();
      this.eligibility = new Eligibility(this.opts.name, this.hiddenGraceMs);
      void this.main(this.run.signal, this.eligibility);
    }
    // A late joiner missed everything so far; have it load initial state now.
    this.fireConnectOrResync(sub);
    return () => {
      this.subscribers.delete(sub);
      if (this.subscribers.size === 0) {
        this.run?.abort();
        this.run = null;
        this.eligibility?.stop();
        this.eligibility = null;
      }
    };
  }

  // The whole algorithm: while eligible, hold the stream; park when another
  // tab claims it or this one is hidden too long; reload around every gap.
  private async main(signal: AbortSignal, eligibility: Eligibility) {
    while (!signal.aborted) {
      if (!eligibility.eligible) {
        await eligibility.changed(signal);
        continue;
      }

      // Aborts the connection when eligibility may have been lost or on
      // teardown; the loop re-checks the real state either way.
      const conn = new AbortController();
      const abortConn = () => conn.abort();
      signal.addEventListener("abort", abortConn, { once: true });
      eligibility.onChange = () => {
        if (!eligibility.eligible) conn.abort();
      };
      try {
        this.deliverConnectOrResync(); // rule 3: reload around every gap
        for await (const msg of this.opts.connect(conn.signal)) {
          this.deliverMessage(msg);
        }
      } catch (err) {
        if (!conn.signal.aborted) {
          console.warn(`[tabStream:${this.opts.name}] stream error`, err);
        }
      } finally {
        eligibility.onChange = null;
        signal.removeEventListener("abort", abortConn);
      }

      // Parked or torn down: the top of the loop handles both. Otherwise the
      // stream itself ended or errored, so back off before reconnecting.
      if (signal.aborted || !eligibility.eligible) continue;
      await abortableDelay(this.backoffMs, signal);
    }
  }

  private deliverMessage(msg: T) {
    for (const sub of this.subscribers) {
      try {
        sub.onMessage(msg);
      } catch (e) {
        console.warn(`[tabStream:${this.opts.name}] onMessage threw`, e);
      }
    }
  }

  private deliverConnectOrResync() {
    for (const sub of this.subscribers) this.fireConnectOrResync(sub);
  }

  private fireConnectOrResync(sub: StreamSubscriber<T>) {
    try {
      sub.onConnectOrResync();
    } catch (e) {
      console.warn(`[tabStream:${this.opts.name}] onConnectOrResync threw`, e);
    }
  }
}

/** A setTimeout that also resolves early if `signal` aborts. */
function abortableDelay(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) return resolve();
    const cleanup = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", onAbort);
    };
    const onAbort = () => {
      cleanup();
      resolve();
    };
    const timer = setTimeout(() => {
      cleanup();
      resolve();
    }, ms);
    signal.addEventListener("abort", onAbort, { once: true });
  });
}
