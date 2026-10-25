// The relay transport is Bun's native fetch: Node cannot reproduce the
// fingerprint that gets past the upstream gate. Starting it on the wrong
// runtime must fail fast with an actionable message, not bind listeners and
// answer every request with 429.
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import path from "node:path";

const REPO_ROOT = path.resolve(import.meta.dirname, "..");

test("server.mjs refuses to run under Node", { timeout: 5_000 }, async () => {
  const child = spawn(process.execPath, ["server.mjs"], {
    cwd: REPO_ROOT,
    env: { ...process.env, RELAY_COUNT: "0" },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => (stdout += chunk));
  child.stderr.on("data", (chunk) => (stderr += chunk));

  const code = await new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", resolve);
  });

  assert.equal(code, 1, `stdout: ${stdout}\nstderr: ${stderr}`);
  assert.match(stderr, /Bun runtime required/);
});
