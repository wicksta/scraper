#!/usr/bin/env node
import "../bootstrap.js";
import { spawn } from "node:child_process";

const args = [];

if (process.env.DATABASE_URL) {
  args.push(process.env.DATABASE_URL);
} else {
  if (process.env.PGHOST) args.push("-h", process.env.PGHOST);
  if (process.env.PGPORT) args.push("-p", process.env.PGPORT);
  if (process.env.PGUSER) args.push("-U", process.env.PGUSER);
  if (process.env.PGDATABASE) args.push("-d", process.env.PGDATABASE);
}

args.push(...process.argv.slice(2));

const child = spawn("psql", args, {
  stdio: "inherit",
  env: process.env,
});

child.on("error", (err) => {
  if (err.code === "ENOENT") {
    console.error("psql not found. Install PostgreSQL client tools and try again.");
    process.exit(1);
  }
  console.error(`Failed to start psql: ${err.message}`);
  process.exit(1);
});

child.on("exit", (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
    return;
  }
  process.exit(code ?? 0);
});
