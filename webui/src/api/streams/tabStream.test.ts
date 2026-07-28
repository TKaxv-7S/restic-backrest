import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  vi,
  type Mock,
} from "vitest";
import { createTabStream, type StreamSubscriber } from "./tabStream";

// Delivers to sibling channels of the same name, never to the sender
// (matching real semantics). A shared registry lets multiple TabStream
// instances in one test stand in for multiple tabs, and lets tests inject
// claim/release messages as if from another tab.
const bcRegistry = new Map<string, Set<MockBroadcastChannel>>();

class MockBroadcastChannel {
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  constructor(public readonly name: string) {
    if (!bcRegistry.has(name)) bcRegistry.set(name, new Set());
    bcRegistry.get(name)!.add(this);
  }
  postMessage(data: unknown) {
    for (const ch of bcRegistry.get(this.name) ?? []) {
      if (ch !== this && ch.onmessage) {
        const handler = ch.onmessage;
        queueMicrotask(() => handler({ data }));
      }
    }
  }
  close() {
    bcRegistry.get(this.name)?.delete(this);
  }
}

// Posts a message into a named channel as if from another tab.
const postAs = (name: string, data: unknown) => {
  const ch = new MockBroadcastChannel(name);
  ch.postMessage(data);
  ch.close();
};

// A never-resolving stream body that unwinds when its signal aborts — stands
// in for a long-lived server-stream that stays open.
const openUntilAborted = (signal: AbortSignal): Promise<void> =>
  new Promise((resolve) => {
    if (signal.aborted) return resolve();
    signal.addEventListener("abort", () => resolve(), { once: true });
  });

type Msg = { id: number };

const streamOf = (msg: Msg) =>
  vi.fn((signal: AbortSignal) =>
    (async function* () {
      yield msg;
      await openUntilAborted(signal);
    })(),
  );

const spySubscriber = (): StreamSubscriber<Msg> & {
  onMessage: Mock;
  onConnectOrResync: Mock;
} => ({
  onMessage: vi.fn(),
  onConnectOrResync: vi.fn(),
});

const tick = (ms = 2) => new Promise((r) => setTimeout(r, ms));

beforeEach(() => {
  bcRegistry.clear();
  vi.stubGlobal("BroadcastChannel", MockBroadcastChannel);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("tabStream", () => {
  it("streams, reconnects with backoff, and fires onConnectOrResync on subscribe and each connect attempt", async () => {
    let call = 0;
    const connect = vi.fn((signal: AbortSignal) => {
      call++;
      const n = call;
      return (async function* () {
        if (n === 1) {
          yield { id: 1 };
          return; // stream ends → triggers a reconnect
        }
        yield { id: 2 };
        await openUntilAborted(signal);
      })();
    });

    const stream = createTabStream<Msg>({
      name: "t1",
      connect,
      backoffMs: 5,
    });

    const sub = spySubscriber();
    const dispose = stream.subscribe(sub);

    await vi.waitFor(() =>
      expect(sub.onMessage).toHaveBeenCalledWith({ id: 2 }),
    );
    expect(sub.onMessage).toHaveBeenCalledWith({ id: 1 });
    // Once on subscribe, then once per connect attempt.
    expect(sub.onConnectOrResync).toHaveBeenCalledTimes(3);

    dispose();
  });

  it("hands the stream to the most recently started tab", async () => {
    const connectA = streamOf({ id: 1 });
    const connectB = streamOf({ id: 2 });
    const streamA = createTabStream<Msg>({ name: "t2", connect: connectA });
    const streamB = createTabStream<Msg>({ name: "t2", connect: connectB });

    const subA = spySubscriber();
    const disposeA = streamA.subscribe(subA);
    await vi.waitFor(() =>
      expect(subA.onMessage).toHaveBeenCalledWith({ id: 1 }),
    );

    // B starts later, so its claim is newer: A stands down, B streams.
    await tick();
    const subB = spySubscriber();
    const disposeB = streamB.subscribe(subB);
    await vi.waitFor(() =>
      expect(subB.onMessage).toHaveBeenCalledWith({ id: 2 }),
    );
    const aSignal = connectA.mock.calls[0][0];
    expect(aSignal.aborted).toBe(true);
    expect(connectA).toHaveBeenCalledTimes(1); // A did not reconnect

    disposeA();
    disposeB();
  });

  it("stands down for a newer claim and reclaims on that tab's release", async () => {
    const connect = streamOf({ id: 1 });
    const stream = createTabStream<Msg>({ name: "t3", connect, backoffMs: 5 });

    const sub = spySubscriber();
    const dispose = stream.subscribe(sub);
    await vi.waitFor(() =>
      expect(sub.onMessage).toHaveBeenCalledWith({ id: 1 }),
    );

    // Another tab claims: this tab's connection aborts and stays down.
    postAs("t3", { t: "claim", ts: Date.now() + 1000, id: "other" });
    await vi.waitFor(() =>
      expect(connect.mock.calls[0][0].aborted).toBe(true),
    );
    await tick(20);
    expect(connect).toHaveBeenCalledTimes(1);

    // That tab goes away: this tab reclaims and reconnects.
    postAs("t3", { t: "release", id: "other" });
    await vi.waitFor(() => expect(connect).toHaveBeenCalledTimes(2));
    await vi.waitFor(() =>
      expect(sub.onMessage).toHaveBeenCalledTimes(2),
    );

    dispose();
  });

  it("releases its claim on teardown so another tab can reclaim", async () => {
    const connectA = streamOf({ id: 1 });
    const connectB = streamOf({ id: 2 });
    const streamA = createTabStream<Msg>({ name: "t4", connect: connectA });
    const streamB = createTabStream<Msg>({ name: "t4", connect: connectB });

    const disposeA = streamA.subscribe(spySubscriber());
    await tick();
    const subB = spySubscriber();
    const disposeB = streamB.subscribe(subB);
    await vi.waitFor(() =>
      expect(subB.onMessage).toHaveBeenCalledWith({ id: 2 }),
    );

    // B (the streamer) tears down: A reclaims and starts streaming.
    disposeB();
    await vi.waitFor(() => expect(connectA).toHaveBeenCalledTimes(2));

    disposeA();
  });

  it("stops streaming when hidden past the grace period and reconnects with a reload on return", async () => {
    let hidden = false;
    Object.defineProperty(document, "hidden", {
      configurable: true,
      get: () => hidden,
    });

    const connect = streamOf({ id: 1 });
    const stream = createTabStream<Msg>({
      name: "t5",
      connect,
      hiddenGraceMs: 20,
    });

    const sub = spySubscriber();
    const dispose = stream.subscribe(sub);
    await vi.waitFor(() =>
      expect(sub.onMessage).toHaveBeenCalledWith({ id: 1 }),
    );
    const before = sub.onConnectOrResync.mock.calls.length;

    hidden = true;
    document.dispatchEvent(new Event("visibilitychange"));
    await vi.waitFor(() =>
      expect(connect.mock.calls[0][0].aborted).toBe(true),
    );

    hidden = false;
    document.dispatchEvent(new Event("visibilitychange"));
    await vi.waitFor(() => expect(connect).toHaveBeenCalledTimes(2));
    expect(sub.onConnectOrResync.mock.calls.length).toBe(before + 1);

    dispose();
    delete (document as unknown as { hidden?: unknown }).hidden;
  });

  it("streams per tab when BroadcastChannel is unavailable", async () => {
    vi.stubGlobal("BroadcastChannel", undefined);

    const connectA = streamOf({ id: 1 });
    const connectB = streamOf({ id: 2 });
    const streamA = createTabStream<Msg>({ name: "t6", connect: connectA });
    const streamB = createTabStream<Msg>({ name: "t6", connect: connectB });

    const subA = spySubscriber();
    const subB = spySubscriber();
    const disposeA = streamA.subscribe(subA);
    const disposeB = streamB.subscribe(subB);

    // No claims can be heard, so both stream for themselves.
    await vi.waitFor(() =>
      expect(subA.onMessage).toHaveBeenCalledWith({ id: 1 }),
    );
    await vi.waitFor(() =>
      expect(subB.onMessage).toHaveBeenCalledWith({ id: 2 }),
    );

    disposeA();
    disposeB();
  });
});
