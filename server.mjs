import { once } from "node:events";
import http from "node:http";
import net from "node:net";
import { Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import { createSignalWriter } from "./signal-writer.mjs";

// Bun's native fetch is the only transport that passes the OpenCode gate:
// Node/undici and Bun's bundled undici both keep the old TLS/HTTP fingerprint
// and are answered with 429 even when the WARP egress itself is clean.
if (typeof Bun === "undefined") {
  await new Promise((resolve) => {
    process.stderr.write("[relay] Bun runtime required: run `bun server.mjs`.\n", resolve);
  });
  process.exit(1);
}

const COUNT = parseInt(process.env.RELAY_COUNT || "10", 10);
// Per-listener in-flight cap. Streaming responses hold sockets for minutes,
// and the relay intentionally has no request timeout, so unbounded fan-in is
// the one way this process can exhaust memory. 0 disables the cap.
const MAX_INFLIGHT = parseInt(process.env.RELAY_MAX_INFLIGHT || "256", 10);
const BASE_PORT = parseInt(process.env.BASE_PORT || "3000", 10);
const HEADER_TARGET = "x-relay-target";
const HEADER_PATH = "x-relay-path";
// keep-cold's probes carry this: they must never appear as live-traffic
// signals, or a probe's own 429 would be replayed to the next pass.
const HEADER_PROBE = "x-keep-cold-probe";
const DEFAULT_ALLOWED_TARGETS = "https://opencode.ai,https://api.kilo.ai,https://oai.endpoints.kepler.ai.cloud.ovh.net";
const ALLOWED_TARGETS = (process.env.RELAY_ALLOWED_TARGETS ?? DEFAULT_ALLOWED_TARGETS)
  .split(",")
  .map((s) => s.trim().replace(/\/$/, ""))
  .filter(Boolean);

/** Canonical host:port used by the loopback proxy's second allowlist gate. */
function authorityOf(target) {
  try {
    const url = new URL(target);
    if (!url.hostname) return null;
    const port = url.port || (url.protocol === "https:" ? "443" : "80");
    return `${url.hostname.toLowerCase()}:${port}`;
  } catch {
    return null;
  }
}

const ALLOWED_AUTHORITIES = new Set(ALLOWED_TARGETS.map(authorityOf).filter(Boolean));
// Health checks are the one deliberate exception: the relay dials the trace
// endpoint through WARP even though it is not part of the client allowlist.
ALLOWED_AUTHORITIES.add("cloudflare.com:443");

// Live rate-limit signals: report the egress/model that just answered 429 so
// keep-cold can rotate only what is actually throttled. Off by default — the
// probe scheduler stays the fallback.
const LIVE_SIGNALS = process.env.LIVE_SIGNALS === "1";
const LIVE_SIGNAL_FILE = process.env.LIVE_SIGNAL_FILE || ".keep-cold/signals.json";
const signalWriter = LIVE_SIGNALS ? createSignalWriter({ file: LIVE_SIGNAL_FILE }) : null;
// Enough to cover a chat-completions preamble (stream flags, tool schemas) on
// every real client; the model field appears before the first message.
const MODEL_SCAN_BYTES = 16 * 1024;
const MODEL_FIELD_RE = /"model"\s*:\s*"([^"]+)"/;

// Request handlers catch their own failures, so an exception that reaches the
// process is a programming error: log it and let Docker restart a clean
// process. Continuing would leave a half-dead relay that `restart: always`
// never replaces.
process.on("unhandledRejection", (err) => {
  console.error(`[relay] unhandledRejection: ${err?.stack ?? String(err)}`);
});
process.on("uncaughtException", (err) => {
  console.error(`[relay] uncaughtException: ${err?.stack ?? String(err)}`);
  signalWriter?.flush();
  process.exit(1);
});

const servers = [];

// Bun's fetch only supports HTTP proxies, while MicroWARP exposes SOCKS5. Each
// relay therefore gets a private loopback HTTP proxy that dials the lane's
// SOCKS5 endpoint. It also re-checks the target authority: a bug in the header
// allowlist must not turn the relay into an open proxy.
function socks5Connect(proxyHost, proxyPort, hostname, port, timeoutMs, callback) {
  const host = hostname.replace(/^\[|\]$/g, "");
  if (!host || !Number.isInteger(port) || port < 1 || port > 65535) {
    callback(new Error("invalid SOCKS5 target"));
    return;
  }
  const socket = net.connect({ host: proxyHost, port: proxyPort });
  let buf = Buffer.alloc(0);
  let stage = 0;
  let settled = false;

  const finish = (err, result) => {
    if (settled) return;
    settled = true;
    clearTimeout(timer);
    socket.removeListener("data", onData);
    socket.removeListener("error", onError);
    if (err) {
      socket.destroy();
      callback(err);
      return;
    }
    // Pause before handing the socket over: bytes that arrived with the SOCKS
    // reply are pushed back into its readable buffer, and the caller's pipe
    // resumes the flow.
    socket.pause();
    if (buf.length) socket.unshift(buf);
    callback(null, result);
  };

  const timer = setTimeout(() => finish(new Error("SOCKS5 connect timeout")), timeoutMs);
  const onError = (err) => finish(err);
  socket.once("error", onError);
  socket.setNoDelay(true);
  socket.once("connect", () => socket.write(Buffer.from([0x05, 0x01, 0x00])));

  function onData(chunk) {
    buf = Buffer.concat([buf, chunk]);
    for (;;) {
      if (stage === 0) {
        if (buf.length < 2) return;
        if (buf[0] !== 0x05 || buf[1] !== 0x00) return finish(new Error("SOCKS5 no-auth rejected"));
        buf = buf.subarray(2);
        stage = 1;
        // Domain names are resolved by the proxy (microsocks), so DNS never
        // leaks to the relay host.
        const family = net.isIP(host);
        let addr;
        if (family === 4) {
          addr = Buffer.from([0x01, ...host.split(".").map(Number)]);
        } else if (family === 6) {
          addr = Buffer.from([0x04, ...ipv6Bytes(host)]);
        } else {
          const name = Buffer.from(host, "utf8");
          if (name.length > 255) return finish(new Error("SOCKS5 target hostname too long"));
          addr = Buffer.concat([Buffer.from([0x03, name.length]), name]);
        }
        const portBuf = Buffer.alloc(2);
        portBuf.writeUInt16BE(port, 0);
        socket.write(Buffer.concat([Buffer.from([0x05, 0x01, 0x00]), addr, portBuf]));
      } else if (stage === 1) {
        if (buf.length < 4) return;
        if (buf[0] !== 0x05 || buf[1] !== 0x00) return finish(new Error(`SOCKS5 CONNECT failed (${buf[1]})`));
        const atyp = buf[3];
        let addrLen;
        if (atyp === 0x01) addrLen = 4;
        else if (atyp === 0x04) addrLen = 16;
        else if (atyp === 0x03) {
          if (buf.length < 5) return;
          addrLen = 1 + buf[4];
        } else return finish(new Error("SOCKS5 bad reply address type"));
        const replyLen = 4 + addrLen + 2;
        if (buf.length < replyLen) return;
        buf = buf.subarray(replyLen);
        return finish(null, socket);
      } else {
        return;
      }
    }
  }
  socket.on("data", onData);
}

function ipv6Bytes(host) {
  const [head, tail] = host.split("::");
  const left = head ? head.split(":") : [];
  const right = tail ? tail.split(":") : [];
  const groups = [...left, ...Array(8 - left.length - right.length).fill("0"), ...right];
  const out = Buffer.alloc(16);
  groups.forEach((g, i) => out.writeUInt16BE(parseInt(g || "0", 16), i * 2));
  return out;
}

// Re-chunk a decoded HTTP request body. The local proxy's node:http server
// de-chunks incoming bodies, so forwarding the original Transfer-Encoding with
// the decoded bytes would corrupt the upstream request.
async function forwardChunked(req, upstream) {
  for await (const chunk of req) {
    if (chunk.length === 0) continue;
    if (!upstream.write(`${chunk.length.toString(16)}\r\n`)) await once(upstream, "drain");
    if (!upstream.write(chunk)) await once(upstream, "drain");
    if (!upstream.write("\r\n")) await once(upstream, "drain");
  }
  upstream.write("0\r\n\r\n");
}

function createWarpProxy(index, warpHost, warpPort) {
  const proxy = http.createServer((req, res) => {
    let target;
    try {
      target = new URL(req.url);
    } catch {
      target = null;
    }
    if (
      !target ||
      target.protocol !== "http:" ||
      target.username ||
      target.password ||
      target.hash
    ) {
      res.writeHead(400, { "content-type": "text/plain; charset=utf-8", connection: "close" });
      req.resume();
      return res.end("Proxy requires an absolute http:// URL\n");
    }

    const authority = authorityOf(target.href);
    if (!authority || !ALLOWED_AUTHORITIES.has(authority)) {
      res.writeHead(403, { "content-type": "text/plain; charset=utf-8", connection: "close" });
      req.resume();
      return res.end("Forbidden proxy target\n");
    }

    const port = Number(target.port || 80);
    const client = res.socket;
    if (!client) {
      res.destroy();
      return;
    }

    socks5Connect(warpHost, warpPort, target.hostname, port, 30_000, (err, upstream) => {
      if (err) {
        console.error(`[relay #${index}] proxy HTTP connect failed: ${err?.message ?? String(err)}`);
        res.writeHead(502, { "content-type": "text/plain; charset=utf-8", connection: "close" });
        req.resume();
        res.end("Proxy upstream unavailable\n");
        return;
      }
      if (client.destroyed) {
        upstream.destroy();
        return;
      }

      const headers = { ...req.headers, host: target.host, connection: "close" };
      delete headers["proxy-connection"];
      delete headers["proxy-authorization"];
      delete headers["keep-alive"];
      delete headers["te"];
      delete headers["trailer"];
      delete headers["upgrade"];
      const chunked = /\bchunked\b/i.test(String(headers["transfer-encoding"] ?? ""));
      if (chunked) delete headers["content-length"];
      else delete headers["transfer-encoding"];

      const lines = [`${req.method} ${target.pathname}${target.search} HTTP/1.1`];
      for (const [name, value] of Object.entries(headers)) {
        if (value === undefined) continue;
        if (Array.isArray(value)) {
          for (const item of value) lines.push(`${name}: ${item}`);
        } else {
          lines.push(`${name}: ${value}`);
        }
      }
      upstream.write(lines.join("\r\n") + "\r\n\r\n");

      const fail = (error) => {
        if (error) console.error(`[relay #${index}] proxy HTTP upstream error: ${error?.message ?? String(error)}`);
        upstream.destroy();
        client.destroy();
      };
      upstream.on("error", fail);
      req.on("error", fail);
      req.on("aborted", () => fail());
      client.on("error", () => upstream.destroy());
      client.on("close", () => upstream.destroy());

      // Raw byte passthrough: the response already has valid HTTP framing, so
      // there is no reason to parse and re-serialise it.
      upstream.pipe(client);
      if (chunked) {
        forwardChunked(req, upstream).catch(fail);
      } else {
        // Keep the write side open for the response; Content-Length or the
        // HTTP method tells the upstream where the request body ends.
        req.pipe(upstream, { end: false });
      }
    });
  });

  proxy.on("connect", (req, client, head) => {
    let parsed;
    try {
      const url = new URL(`http://${req.url}`);
      if (
        !url.port ||
        url.username ||
        url.password ||
        url.pathname !== "/" ||
        url.search ||
        url.hash
      ) {
        throw new Error("invalid CONNECT authority");
      }
      parsed = {
        authority: `${url.hostname.toLowerCase()}:${url.port}`,
        host: url.hostname,
        port: Number(url.port),
      };
    } catch {
      client.end("HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n");
      return;
    }

    if (!ALLOWED_AUTHORITIES.has(parsed.authority)) {
      client.end("HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n");
      return;
    }

    socks5Connect(warpHost, warpPort, parsed.host, parsed.port, 30_000, (err, upstream) => {
      if (err) {
        console.error(`[relay #${index}] proxy CONNECT failed: ${err?.message ?? String(err)}`);
        client.end("HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\n\r\n");
        return;
      }
      if (client.destroyed) {
        upstream.destroy();
        return;
      }

      client.write("HTTP/1.1 200 Connection Established\r\n\r\n");
      if (head.length) upstream.write(head);
      upstream.on("error", () => client.destroy());
      client.on("error", () => upstream.destroy());
      client.on("close", () => upstream.destroy());
      upstream.pipe(client);
      client.pipe(upstream);
    });
  });

  proxy.on("clientError", (_err, socket) => {
    if (socket.writable) socket.end("HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n");
    else socket.destroy();
  });
  proxy.on("connection", (socket) => socket.setNoDelay(true));
  return proxy;
}

const DROP_HEADERS = new Set([
  "host",
  HEADER_TARGET,
  HEADER_PATH,
  HEADER_PROBE,
  "connection",
  "keep-alive",
  "proxy-connection",
  "transfer-encoding",
  "te",
  "trailer",
  "upgrade",
  "expect",
  // Bun fetch decodes gzip/deflate/br and the relay strips content-encoding,
  // so let it advertise only encodings it can always decode.
  "accept-encoding",
]);

// The upstream status alone does not say which model was throttled, and the
// body cannot be buffered (streaming requests, memory). This tee reads the
// model field out of the first MODEL_SCAN_BYTES and forwards every byte.
function createModelSniffer() {
  let seen = "";
  const sniffer = new Transform({
    transform(chunk, _enc, done) {
      if (sniffer.model === null && seen.length < MODEL_SCAN_BYTES) {
        // latin1 keeps one char per byte when a chunk splits a UTF-8 rune;
        // model ids are ASCII, so the pattern still matches.
        seen += chunk.subarray(0, MODEL_SCAN_BYTES - seen.length).toString("latin1");
        const match = MODEL_FIELD_RE.exec(seen);
        if (match) sniffer.model = match[1];
      }
      done(null, chunk);
    },
  });
  sniffer.model = null;
  return sniffer;
}

async function createRelayInstance(index) {
  const port = BASE_PORT + index;
  const warpHost = process.env[`WARP_HOST_${index}`] || `warp-${index}`;
  const warpPort = process.env[`WARP_PORT_${index}`] || "1080";
  const warpEndpoint = `socks5://${warpHost}:${warpPort}`;

  // Bind the adapter before the public listener so a failed loopback bind is
  // a clean startup failure, not a listener that 502s every request.
  const localProxy = createWarpProxy(index, warpHost, warpPort);
  await new Promise((resolve, reject) => {
    const onError = (err) => {
      localProxy.removeListener("listening", onListening);
      reject(err);
    };
    const onListening = () => {
      localProxy.removeListener("error", onError);
      resolve();
    };
    localProxy.once("error", onError);
    localProxy.once("listening", onListening);
    localProxy.listen(0, "127.0.0.1");
  });
  localProxy.on("error", (err) => {
    console.error(`[relay #${index}] fatal proxy error: ${err?.stack ?? String(err)}`);
    signalWriter?.flush();
    process.exit(1);
  });
  const localProxyUrl = `http://127.0.0.1:${localProxy.address().port}`;
  // respectNoProxy:false matters: the host may export NO_PROXY=* or list the
  // upstream domains, and fetching directly would silently bypass WARP.
  const proxyConfig = { url: localProxyUrl, respectNoProxy: false };

  let inflight = 0;

  const server = http.createServer(async (req, res) => {
    // A client that vanishes mid-response must not surface as an unhandled
    // 'error' event on either side.
    req.on("error", () => {});
    res.on("error", () => {});

    if (MAX_INFLIGHT > 0 && inflight >= MAX_INFLIGHT) {
      res.writeHead(503, { "content-type": "application/json", "retry-after": "5" });
      return res.end(JSON.stringify({ error: "Relay busy" }));
    }
    inflight += 1;
    res.once("close", () => {
      inflight -= 1;
    });

    const target = req.headers[HEADER_TARGET]?.replace(/\/$/, "");
    const relayPath = req.headers[HEADER_PATH] || "/";

    // Direct health check: only when NO x-relay-target header is passed
    if (!target) {
      try {
        const trace = await fetch("https://cloudflare.com/cdn-cgi/trace", {
          proxy: proxyConfig,
          signal: AbortSignal.timeout(5_000),
        });
        if (!trace.ok) throw new Error(`trace HTTP ${trace.status}`);
        const text = await trace.text();
        res.writeHead(200, { "content-type": "text/plain; charset=utf-8" });
        return res.end(`Relay #${index} OK (via WARP)\n\n${text}`);
      } catch (err) {
        // Detail stays in the logs: the body used to reflect String(err).
        console.error(`[relay #${index}] health check failed: ${err?.stack ?? String(err)}`);
        res.writeHead(500, { "content-type": "text/plain; charset=utf-8" });
        return res.end(`Relay #${index} health check failed\n`);
      }
    }

    if (!ALLOWED_TARGETS.includes(target)) {
      res.writeHead(403, { "content-type": "application/json" });
      return res.end(JSON.stringify({ error: "Forbidden target" }));
    }

    // The allowlist check above only validates the raw header string; the
    // request URL is target + relayPath, and a crafted path can continue the
    // host label (".attacker.tld/x" -> opencode.ai.attacker.tld), swap the
    // port (":8443/x"), or inject userinfo ("@evil.tld/x"). Build the URL
    // once and require it to stay on the allowlisted origin with a rooted
    // path. Concat (not URL-relative resolution) preserves the legacy
    // target+path semantics for any allowlist entry carrying a base path.
    let upstreamUrl;
    let targetOrigin;
    try {
      targetOrigin = new URL(target).origin;
      upstreamUrl = new URL(target + relayPath);
    } catch {
      upstreamUrl = null;
    }
    if (!upstreamUrl || upstreamUrl.origin !== targetOrigin || !relayPath.startsWith("/")) {
      console.error(`[relay #${index}] rejected relay path: target=${target} path=${relayPath}`);
      res.writeHead(400, { "content-type": "application/json" });
      return res.end(JSON.stringify({ error: "Invalid relay path" }));
    }

    const headers = new Headers();
    for (const [k, v] of Object.entries(req.headers)) {
      if (!DROP_HEADERS.has(k) && v !== undefined) {
        headers.set(k, v);
      }
    }

    // 5-min upstream cap + client-disconnect wiring: a client that goes away
    // must not leave its upstream fetch hanging (leaked sockets pile up under
    // agent retries). Note req 'close' also fires on normal POST body
    // completion (socket still alive), so only abort when the socket actually
    // died; res 'close' covers disconnect-while-waiting for all methods.
    const controller = new AbortController();
    const onClientClose = () => {
      if (!res.writableEnded && req.socket?.destroyed) controller.abort();
    };
    req.once("close", onClientClose);
    res.once("close", onClientClose);
    const upstreamTimeout = AbortSignal.timeout(300000);
    // Only bodies can name a model; with signals off the request object is
    // handed to fetch exactly as before. Probe traffic is never signalled: the
    // controller would otherwise chase throttles its own probes reported.
    const hasBody = req.method !== "GET" && req.method !== "HEAD";
    const isProbe = req.headers[HEADER_PROBE] !== undefined;
    const sniffer = signalWriter && hasBody && !isProbe ? createModelSniffer() : null;
    try {
      const upstream = await fetch(upstreamUrl, {
        method: req.method,
        headers,
        body: hasBody ? Readable.toWeb(sniffer ? req.pipe(sniffer) : req) : undefined,
        duplex: "half",
        // The allowlist constrains the first hop only; following a redirect
        // would let an allowlisted origin steer the relay to any host.
        redirect: "error",
        proxy: proxyConfig,
        signal: AbortSignal.any([upstreamTimeout, controller.signal]),
      });

      if (signalWriter && !isProbe) {
        // Best-effort and additive: a signal failure must never reach the
        // response path.
        try {
          const model = sniffer?.model ?? "unknown";
          if (upstream.status === 429) {
            signalWriter.record({
              relay: index,
              model,
              status: 429,
              retryAfter: Number(upstream.headers.get("retry-after")) || null,
            });
          } else if (upstream.status === 200) {
            signalWriter.record({ relay: index, model, status: 200 });
          }
        } catch {
          // Ignored: keep-cold falls back to probing.
        }
      }

      // Forward only what the client needs: full header copies cost per-request
      // work and risk stale content-encoding/content-length (fetch decodes
      // gzip/deflate/br, so those upstream values lie about our body).
      const outHeaders = { "cache-control": "no-cache", "x-accel-buffering": "no" };
      const contentType = upstream.headers.get("content-type");
      if (contentType) outHeaders["content-type"] = contentType;
      const retryAfter = upstream.headers.get("retry-after");
      if (retryAfter) outHeaders["retry-after"] = retryAfter;
      res.writeHead(upstream.status, outHeaders);
      // Kickstart: Node buffers headers until the first body byte, so TTFB
      // would wait for the first token. Flush now — headers land at
      // upstream-headers time instead.
      res.flushHeaders();
      if (upstream.body) {
        // Zero-copy pipe with backpressure; for-await + res.write() costs a
        // microtask per chunk and buffers against a slow client.
        await pipeline(Readable.fromWeb(upstream.body), res);
      } else {
        res.end();
      }
    } catch (err) {
      if (!res.headersSent) {
        // Body stays generic: String(err) reflected attacker-controlled URLs
        // and internal proxy names back to the caller.
        console.error(`[relay #${index}] upstream error: ${err?.stack ?? String(err)}`);
        res.writeHead(502, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: "upstream request failed" }));
      } else {
        // Headers already on the wire (mid-stream failure) — just cut it.
        // Unconditional writeHead here crashed the process (ERR_HTTP_HEADERS_SENT).
        res.destroy();
      }
    } finally {
      req.removeListener("close", onClientClose);
      res.removeListener("close", onClientClose);
    }
  });

  // TTFT tuning: Nagle off (first small frame goes immediately), long
  // keep-alive so cloudflared reuses the relay-app connection.
  server.keepAliveTimeout = 90_000;
  server.headersTimeout = 95_000;
  server.requestTimeout = 0;
  server.timeout = 0;
  server.on("connection", (socket) => socket.setNoDelay(true));
  // A failed bind must be fatal: a half-started relay with missing listeners
  // is invisible to `restart: always` and only shows up as per-port 502s.
  server.on("error", (err) => {
    console.error(`[relay #${index}] fatal listen error: ${err?.stack ?? String(err)}`);
    signalWriter?.flush();
    process.exit(1);
  });

  servers.push(localProxy);
  servers.push(server);
  server.listen(port, "0.0.0.0", () => {
    console.log(`Relay #${index} listening on 0.0.0.0:${port} -> ${localProxyUrl} -> ${warpEndpoint}`);
  });
}

for (let i = 1; i <= COUNT; i++) {
  try {
    await createRelayInstance(i);
  } catch (err) {
    console.error(`[relay #${i}] fatal startup error: ${err?.stack ?? String(err)}`);
    signalWriter?.flush();
    process.exit(1);
  }
}

// Drain on deploy: stop accepting, let in-flight streams finish, then force
// the exit so a wedged connection cannot hold the container open.
let shuttingDown = false;
function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`[relay] ${signal} received — draining ${servers.length} listener(s)`);
  signalWriter?.flush();
  for (const server of servers) {
    server.close();
    server.closeIdleConnections?.();
  }
  setTimeout(() => process.exit(0), 10_000).unref();
}
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
