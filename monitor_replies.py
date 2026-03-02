from __future__ import annotations

from pathlib import Path

from googleapiclient.errors import HttpError

from email_campaign_core import (
    get_gmail_service,
    load_tracking_state,
    refresh_record_reply_status,
    safe_http_error_message,
    save_tracking_state,
)

CREDENTIALS_FILE = Path("credentials.json")
TOKEN_FILE = Path("token.json")
TRACKING_FILE = Path("email_tracking_state.json")


def main() -> None:
    """Check each tracked thread and mark recipients that replied."""
    try:
        gmail_service = get_gmail_service(CREDENTIALS_FILE, TOKEN_FILE)
        tracking_records = load_tracking_state(TRACKING_FILE)

        if not tracking_records:
            print("No tracking records found. Run send_emails.py first.")
            return

        checked_count = 0
        new_replies_count = 0

        for record in tracking_records.values():
            checked_count += 1
            try:
                reply_found = refresh_record_reply_status(gmail_service, record)
                if reply_found:
                    new_replies_count += 1
                    print(f"Reply detected: {record.recipient}")
            except HttpError as error:
                print(
                    f"Failed to inspect {record.recipient}: "
                    f"{safe_http_error_message(error)}"
                )

        save_tracking_state(TRACKING_FILE, tracking_records)
        total_replied = sum(1 for item in tracking_records.values() if item.replied)
        total_pending = sum(1 for item in tracking_records.values() if not item.replied)

        print(f"Checked threads: {checked_count}")
        print(f"New replies detected now: {new_replies_count}")
        print(f"Total replied: {total_replied}")
        print(f"Total pending: {total_pending}")
        print(f"Tracking file updated: {TRACKING_FILE.resolve()}")
    except FileNotFoundError as error:
        print(f"File not found: {error.filename}")
    except ValueError as error:
        print(f"Invalid input: {error}")
    except Exception as error:  # pragma: no cover
        print(f"Unexpected error: {error}")


if __name__ == "__main__":
    main()
