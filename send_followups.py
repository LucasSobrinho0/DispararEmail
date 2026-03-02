from __future__ import annotations

import argparse
from pathlib import Path

from googleapiclient.errors import HttpError

from email_campaign_core import (
    get_gmail_service,
    load_tracking_state,
    safe_http_error_message,
    save_tracking_state,
    send_follow_up_email,
    should_send_follow_up,
)

CREDENTIALS_FILE = Path("credentials.json")
TOKEN_FILE = Path("token.json")
TRACKING_FILE = Path("email_tracking_state.json")

FOLLOW_UP_SUBJECT = "Re: teste"
FOLLOW_UP_BODY = "Hello, I am following up on my previous email."
DEFAULT_FOLLOW_UP_AFTER_HOURS = 24


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Send follow-up emails for recipients that did not reply.",
    )
    parser.add_argument(
        "--after-hours",
        type=int,
        default=DEFAULT_FOLLOW_UP_AFTER_HOURS,
        help="Minimum hours to wait after initial email before follow-up.",
    )
    return parser.parse_args()


def main() -> None:
    """Send one follow-up email per recipient when eligible."""
    args = parse_args()

    try:
        gmail_service = get_gmail_service(CREDENTIALS_FILE, TOKEN_FILE)
        tracking_records = load_tracking_state(TRACKING_FILE)

        if not tracking_records:
            print("No tracking records found. Run send_emails.py first.")
            return

        follow_ups_sent = 0
        skipped_without_monitoring = 0

        for record in tracking_records.values():
            # Follow-up depends on monitoring output to avoid blind resends.
            if not record.last_checked_at:
                skipped_without_monitoring += 1
                print(
                    f"Skipping {record.recipient}: monitor_replies.py has not checked it yet."
                )
                continue

            if not should_send_follow_up(record, args.after_hours):
                continue

            try:
                send_follow_up_email(
                    service=gmail_service,
                    record=record,
                    subject=FOLLOW_UP_SUBJECT,
                    body=FOLLOW_UP_BODY,
                )
                follow_ups_sent += 1
                print(f"Follow-up sent to {record.recipient}")
            except HttpError as error:
                print(
                    f"Failed to send follow-up to {record.recipient}: "
                    f"{safe_http_error_message(error)}"
                )

        save_tracking_state(TRACKING_FILE, tracking_records)
        print(f"Follow-ups sent: {follow_ups_sent}")
        print(f"Skipped (missing monitoring step): {skipped_without_monitoring}")
        print(f"Tracking file updated: {TRACKING_FILE.resolve()}")
    except FileNotFoundError as error:
        print(f"File not found: {error.filename}")
    except ValueError as error:
        print(f"Invalid input: {error}")
    except Exception as error:  # pragma: no cover
        print(f"Unexpected error: {error}")


if __name__ == "__main__":
    main()
