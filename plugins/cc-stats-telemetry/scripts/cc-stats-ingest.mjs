#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import os from "node:os";
import path from "node:path";

async function readStdin() {
  const chunks = [];
  for await (const chunk of process.stdin) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString("utf8");
}

function launcherCandidates() {
  const home = os.homedir();
  const appdata = process.env.APPDATA || path.join(home, "AppData", "Roaming");
  return [
    process.env.CC_STATS_LAUNCHER,
    process.platform === "win32" ? path.join(appdata, "cc-stats", "bin", "cc-stats.cmd") : null,
    process.platform === "win32" ? path.join(home, ".local", "share", "cc-stats", "bin", "cc-stats.cmd") : null,
    process.platform === "win32" ? null : path.join(home, ".local", "share", "cc-stats", "bin", "cc-stats"),
    "cc-stats"
  ].filter(Boolean);
}

function canTry(command) {
  if (command === "cc-stats") {
    return true;
  }
  return existsSync(command);
}

function invoke(command, payload) {
  return spawnSync(command, ["client", "ingest-claude-hook"], {
    input: payload,
    stdio: ["pipe", "inherit", "inherit"],
    env: process.env,
    shell: process.platform === "win32"
  });
}

const payload = await readStdin();

for (const command of launcherCandidates()) {
  if (!canTry(command)) {
    continue;
  }
  const result = invoke(command, payload);
  if (result.error && result.error.code === "ENOENT") {
    continue;
  }
  if (result.error) {
    console.error(`cc-stats plugin hook failed via ${command}: ${result.error.message}`);
    process.exit(1);
  }
  process.exit(result.status ?? 0);
}

console.error("cc-stats plugin hook could not find a launcher. Run `uv run cc-stats client install --server-url ...` first.");
process.exit(1);
