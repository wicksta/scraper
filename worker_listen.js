// worker_listen.js
import "./bootstrap.js";
import pg from "pg";
const { Client } = pg;

const CHANNEL = "scrape_job_created";

let client;

async function connectAndListen() {
  const clientConfig = process.env.DATABASE_URL
    ? { connectionString: process.env.DATABASE_URL }
    : {
        host: process.env.PGHOST,
        port: process.env.PGPORT ? Number(process.env.PGPORT) : undefined,
        database: process.env.PGDATABASE,
        user: process.env.PGUSER,
        password: process.env.PGPASSWORD,
      };

  client = new Client(clientConfig);

  client.on("error", (err) => {
    console.error("[pg] client error:", err);
  });

  client.on("end", () => {
    console.error("[pg] connection ended; reconnecting in 2s...");
    setTimeout(connectAndListen, 2000);
  });

  await client.connect();
  console.log("[pg] connected");

  // Important: if connection drops, LISTEN is lost; hence reconnect logic above.
  await client.query(`LISTEN ${CHANNEL}`);
  console.log(`[pg] LISTEN ${CHANNEL}`);

  client.on("notification", (msg) => {
    if (msg.channel !== CHANNEL) return;
    console.log("[pg] notification:", msg.payload);
    // next: claim job + run scraper
  });
}

connectAndListen().catch((e) => {
  console.error("[worker] fatal connect error:", e);
  process.exit(1);
});
