# Warp Relay Pool OpenCode Free-Tier Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor the warp-relay pool architecture to distribute distinct Cloudflare Edge WireGuard endpoints across containers, implement a persistent subnet blacklist cache in the rotation controller, and ensure rapid, resilient routing for OpenCode free-tier models (`deepseek-v4.1-flash-free`, `glm-5.3-flash-free`, `mimo-v2.6-flash-free`).

**Architecture:** Assign distinct Cloudflare endpoint IPs across containers in `generate-compose.sh`. Implement `throttled-prefixes.json` blacklist tracking in `keep-cold.mjs` to skip known-exhausted `/64` subnets during fast resets without making upstream requests. Re-deploy on `acerblue-ts` and verify.

**Tech Stack:** Node.js (ESM), Docker Compose, WireGuard (`wg-quick`), Cloudflare WARP REST Client API, Bash.

**Spec:** `docs/superpowers/specs/2026-10-25-warp-relay-opencode-refactor-design.md`

## Global Constraints
- Target host is `acerblue-ts` (`100.124.182.101`).
- All 10 relay ports are `3101..3110`, SOCKS ports are `10801..10810`.
- All requests must carry the official OpenCode client fingerprint (`stream: true`, 6-tool sextet, `accept: text/event-stream`, `Authorization: Bearer public`).
- Pre-push gates (`sh -n`, `node --check`, `node --test`, `gitleaks`, `trufflehog`) must pass 100%.

---

### Task 1: Update `generate-compose.sh` with Diverse Cloudflare Endpoints

**Files:**
- Modify: `generate-compose.sh`
- Test: `bash generate-compose.sh 10 && grep ENDPOINT_IP docker-compose.yml`

- [ ] **Step 1: Check existing `generate-compose.sh` endpoint injection**
- [ ] **Step 2: Add endpoint pool array and assign distinct `ENDPOINT_IP` per container**
- [ ] **Step 3: Run `sh -n generate-compose.sh` and test generation locally**
- [ ] **Step 4: Commit changes to git**

---

### Task 2: Implement Subnet Blacklist Cache in `keep-cold.mjs`

**Files:**
- Modify: `keep-cold.mjs`
- Test: `test/keep-cold.test.mjs`

- [ ] **Step 1: Write unit tests in `test/keep-cold.test.mjs` covering prefix blacklist cache**
- [ ] **Step 2: Run test to verify it fails**
- [ ] **Step 3: Implement `loadThrottledPrefixes()`, `saveThrottledPrefixes()`, and pre-probe check in `keep-cold.mjs`**
- [ ] **Step 4: Run `node --test test/keep-cold.test.mjs` to verify it passes**
- [ ] **Step 5: Commit changes to git**

---

### Task 3: Deploy to `acerblue-ts` and Recreate 10 Diverse Relays

**Files:**
- Deploy to: `acerblue-ts:/home/trefeon/warp-relay/`

- [ ] **Step 1: Scp `generate-compose.sh` and `keep-cold.mjs` to `acerblue-ts`**
- [ ] **Step 2: Run `INGRESS_BIND=0.0.0.0 ./generate-compose.sh 10` on `acerblue-ts`**
- [ ] **Step 3: Recreate containers: `docker compose up -d`**
- [ ] **Step 4: Verify distinct `/64` egress IPs across all 10 containers**

---

### Task 4: Run Live Verification Sweep and Validate pi-freeflow Routing

**Files:**
- Run probe: `node keep-cold.mjs --probe` on `acerblue-ts`
- Run local test: `curl http://127.0.0.1:28180/v1/chat/completions`

- [ ] **Step 1: Run baseline status probe across all 10 relays on `acerblue-ts`**
- [ ] **Step 2: Run verification test through local `pi-freeflow` proxy**
- [ ] **Step 3: Run all pre-push verification gates**
- [ ] **Step 4: Final commit and push to `main`**
