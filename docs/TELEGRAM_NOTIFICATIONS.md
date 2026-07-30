# Telegram Trade Notifications

Telegram notifications are optional and disabled unless both variables below
are present in the local `.env` file:

```text
TELEGRAM_BOT_TOKEN=replace_with_botfather_token
TELEGRAM_CHAT_ID=replace_with_chat_id
```

Completed live entry, completed live exit, final PnL, entry-blocked,
quote-failure, manual-review, and runtime-fuse events are sent. Dry-run and
shadow events are not sent.

Repeated entry-blocked and quote-failure alerts with the same asset and reason
are combined and limited to one message every 30 minutes. The next alert
includes the number of suppressed repeats. This can be changed without changing
code:

```text
TELEGRAM_ALERT_THROTTLE_SECONDS=1800
```

Notification delivery uses a background queue. Telegram latency or failure does
not block exchange submission, change strategy decisions, or stop the runtime.

## Setup

1. Create a bot with Telegram `@BotFather`.
2. Send the new bot a message.
3. Add only `TELEGRAM_BOT_TOKEN` to `.env`.
4. Discover the chat ID:

   ```bash
   python tools/telegram_notify.py --discover-chat-id
   ```

5. Add the returned `TELEGRAM_CHAT_ID` to `.env`.
6. Send a test message:

   ```bash
   python tools/telegram_notify.py
   ```

The live process reads these variables at startup, so restart it after changing
`.env`.
