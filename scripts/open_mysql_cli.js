#!/usr/bin/env node
import "../bootstrap.js";
import { spawn } from "node:child_process";

const host = process.env.MYSQL_HOST;
const port = process.env.MYSQL_PORT;
const user = process.env.MYSQL_USER;
const password = process.env.MYSQL_PASSWORD;
const database = process.env.MYSQL_DATABASE;

if (!user) {
  console.error("Missing MYSQL_USER in environment.");
  process.exit(1);
}

const args = [];
if (host) args.push("--host", host);
if (port) args.push("--port", port);
args.push("--user", user);
if (database) args.push("--database", database);
args.push(...process.argv.slice(2));

const child = spawn("mysql", args, {
  stdio: "inherit",
  env: {
    ...process.env,
    ...(password ? { MYSQL_PWD: password } : {}),
  },
});

child.on("error", (err) => {
  if (err.code === "ENOENT") {
    console.error("mysql client not found. Install MySQL client tools and try again.");
    process.exit(1);
  }
  console.error(`Failed to start mysql: ${err.message}`);
  process.exit(1);
});

child.on("exit", (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
    return;
  }
  process.exit(code ?? 0);
});
