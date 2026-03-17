# Mail Handler

Watches an IMAP inbox, classifies inbound messages for document generation, runs them through the Otso document-generation stack, renders a Word document on Otso, and replies with the generated `.docx` attached.

## Run

```bash
node mail_handler/watch_inbox.js
```

Or:

```bash
npm run mail:watch
```

## Required `.env` keys

```env
MAIL_HANDLER_IMAP_HOST=imap.example.com
MAIL_HANDLER_IMAP_PORT=993
MAIL_HANDLER_IMAP_SECURE=true
MAIL_HANDLER_IMAP_USER=inbox@example.com
MAIL_HANDLER_IMAP_PASSWORD=secret
MAIL_HANDLER_IMAP_MAILBOX=INBOX

MAIL_HANDLER_SMTP_HOST=smtp.example.com
MAIL_HANDLER_SMTP_PORT=587
MAIL_HANDLER_SMTP_SECURE=false
MAIL_HANDLER_SMTP_USER=inbox@example.com
MAIL_HANDLER_SMTP_PASSWORD=secret

MAIL_HANDLER_FROM_ADDRESS=inbox@example.com
MAIL_HANDLER_FROM_NAME=Mail Handler
MAIL_HANDLER_POLL_MS=30000
MAIL_HANDLER_STATE_FILE=/opt/scraper/mail_handler/reply_state.json
MAIL_HANDLER_DOCGEN_START_URL=http://127.0.0.1/document_generation_start.php
MAIL_HANDLER_DOCGEN_API_KEY=shared-internal-key
```

## Optional

```env
MAIL_HANDLER_CLASSIFIER_MODEL=gpt-4o-mini
MAIL_HANDLER_DOCGEN_POLL_MS=1500
MAIL_HANDLER_DOCGEN_TIMEOUT_MS=180000
MAIL_HANDLER_SMTP_VERIFY_TIMEOUT_MS=15000
MAIL_HANDLER_IMAP_CONNECT_TIMEOUT_MS=20000
MAIL_HANDLER_IMAP_OPEN_TIMEOUT_MS=15000
MAIL_HANDLER_IMAP_RECONNECT_DELAY_MS=5000
```

## Behaviour

- Watches the configured IMAP mailbox for `UNSEEN` messages.
- Extracts the inbound email body.
- Uses OpenAI to choose `note`, `letter`, or `minutes`, and prepares `notes_text`.
- Starts the local Otso document-generation job and polls `app_ingest_jobs`.
- Renders the returned structured JSON into a local Word document on Otso.
- Sends the generated `.docx` back by SMTP as an attachment.
- Marks the original message as `\\Seen` and `\\Answered` after a successful reply.
- Marks failed messages as `\\Seen` and records the error in the local state file to avoid repeated retries on every poll.
- Stores handled message ids in a local state file to avoid duplicate processing across restarts.
- Skips messages sent from the reply account itself.
- Skips messages marked as auto-submitted.
- Skips mailing-list style messages and obvious `no-reply` senders.
- Reconnects to IMAP if the connection drops.

## Sender allowlist

The watcher only replies to senders matching one of:

- any address ending in `@nmrk.com`
- any address ending in `@geraldeve.com`
- `james@wickhams.co.uk`

All other senders are ignored.

## Current working config on this server

For `mail.ngist.app`, SMTP port `465` timed out from this server, but SMTP submission on `587` with STARTTLS worked.
