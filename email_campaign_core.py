from __future__ import annotations

import base64
import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from email.utils import parseaddr
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError

SCOPES = [
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.readonly",
]


@dataclass
class EmailTrackingRecord:
    recipient: str
    thread_id: str
    initial_message_id: str
    initial_rfc822_message_id: str | None
    initial_sent_at: str
    replied: bool = False
    replied_at: str | None = None
    follow_up_sent: bool = False
    follow_up_message_id: str | None = None
    follow_up_sent_at: str | None = None
    last_checked_at: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "EmailTrackingRecord":
        return cls(
            recipient=data["recipient"],
            thread_id=data["thread_id"],
            initial_message_id=data["initial_message_id"],
            initial_rfc822_message_id=data.get("initial_rfc822_message_id"),
            initial_sent_at=data["initial_sent_at"],
            replied=bool(data.get("replied", False)),
            replied_at=data.get("replied_at"),
            follow_up_sent=bool(data.get("follow_up_sent", False)),
            follow_up_message_id=data.get("follow_up_message_id"),
            follow_up_sent_at=data.get("follow_up_sent_at"),
            last_checked_at=data.get("last_checked_at"),
        )


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def normalize_email(value: str) -> str:
    return parseaddr(value)[1].strip().lower()


def parse_internal_date(internal_date: str | None) -> str | None:
    if not internal_date:
        return None
    timestamp = datetime.fromtimestamp(int(internal_date) / 1000, tz=timezone.utc)
    return timestamp.isoformat()


def load_recipients(csv_file: Path) -> list[str]:
    """Load recipient emails from a CSV file that contains an 'email' column."""
    recipients: list[str] = []
    seen: set[str] = set()

    with csv_file.open(mode="r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)

        if not reader.fieldnames:
            raise ValueError("The recipients CSV file is empty.")

        field_mapping = {name.strip().lower(): name for name in reader.fieldnames}
        email_column = field_mapping.get("email")
        if not email_column:
            raise ValueError("The recipients CSV file must include an 'email' column.")

        for row in reader:
            email_address = (row.get(email_column) or "").strip()
            normalized = normalize_email(email_address)
            if normalized and normalized not in seen:
                recipients.append(email_address)
                seen.add(normalized)

    if not recipients:
        raise ValueError("No valid email addresses were found in the CSV file.")

    return recipients


def load_tracking_state(tracking_file: Path) -> dict[str, EmailTrackingRecord]:
    """Load tracking state from disk. Returns an empty dictionary if missing."""
    if not tracking_file.exists():
        return {}

    raw_content = tracking_file.read_text(encoding="utf-8")
    if not raw_content.strip():
        return {}

    data = json.loads(raw_content)
    if not isinstance(data, dict):
        raise ValueError("Tracking file format is invalid. Expected a JSON object.")

    records: dict[str, EmailTrackingRecord] = {}
    for recipient_key, payload in data.items():
        if isinstance(payload, dict):
            records[normalize_email(recipient_key)] = EmailTrackingRecord.from_dict(payload)

    return records


def save_tracking_state(
    tracking_file: Path,
    records: dict[str, EmailTrackingRecord],
) -> None:
    """Persist tracking state on disk in a human-readable JSON format."""
    serialized = {key: asdict(records[key]) for key in sorted(records.keys())}
    tracking_file.write_text(
        json.dumps(serialized, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def get_gmail_service(credentials_file: Path, token_file: Path) -> Resource:
    """Authenticate with Gmail API and return a Gmail service client."""
    credentials: Credentials | None = None

    if token_file.exists():
        credentials = Credentials.from_authorized_user_file(str(token_file), SCOPES)
        if credentials.scopes and not set(SCOPES).issubset(set(credentials.scopes)):
            credentials = None

    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(credentials_file), SCOPES)
            credentials = flow.run_local_server(port=0)

        token_file.write_text(credentials.to_json(), encoding="utf-8")

    return build("gmail", "v1", credentials=credentials)


def build_message(
    recipient: str,
    subject: str,
    body: str,
    in_reply_to: str | None = None,
) -> str:
    """Build and base64-encode a MIME message for Gmail API."""
    message = MIMEText(body, _charset="utf-8")
    message["to"] = recipient
    message["subject"] = subject

    if in_reply_to:
        message["In-Reply-To"] = in_reply_to
        message["References"] = in_reply_to

    return base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")


def send_message(
    service: Resource,
    raw_message: str,
    thread_id: str | None = None,
) -> dict:
    """Send an email message using Gmail API."""
    request_body: dict[str, str] = {"raw": raw_message}
    if thread_id:
        request_body["threadId"] = thread_id

    return (
        service.users()
        .messages()
        .send(userId="me", body=request_body)
        .execute()
    )


def get_message_header(message_payload: dict, header_name: str) -> str | None:
    headers = message_payload.get("payload", {}).get("headers", [])
    for header in headers:
        if header.get("name", "").lower() == header_name.lower():
            return header.get("value")
    return None


def get_message_metadata(service: Resource, message_id: str) -> dict:
    """Fetch message metadata with headers needed for tracking."""
    return (
        service.users()
        .messages()
        .get(
            userId="me",
            id=message_id,
            format="metadata",
            metadataHeaders=["From", "Message-ID"],
        )
        .execute()
    )


def send_initial_email(
    service: Resource,
    recipient: str,
    subject: str,
    body: str,
) -> EmailTrackingRecord:
    """Send first contact email and return tracking data."""
    raw_message = build_message(recipient=recipient, subject=subject, body=body)
    response = send_message(service=service, raw_message=raw_message)

    message_id = response.get("id", "")
    thread_id = response.get("threadId", "")

    metadata = get_message_metadata(service, message_id)
    rfc822_message_id = get_message_header(metadata, "Message-ID")
    initial_sent_at = parse_internal_date(metadata.get("internalDate")) or utc_now_iso()

    return EmailTrackingRecord(
        recipient=recipient,
        thread_id=thread_id,
        initial_message_id=message_id,
        initial_rfc822_message_id=rfc822_message_id,
        initial_sent_at=initial_sent_at,
    )


def refresh_record_reply_status(service: Resource, record: EmailTrackingRecord) -> bool:
    """Check thread activity and return True when a new reply is detected."""
    if record.replied:
        record.last_checked_at = utc_now_iso()
        return False

    thread = (
        service.users()
        .threads()
        .get(
            userId="me",
            id=record.thread_id,
            format="metadata",
            metadataHeaders=["From"],
        )
        .execute()
    )

    known_sent_ids = {record.initial_message_id}
    if record.follow_up_message_id:
        known_sent_ids.add(record.follow_up_message_id)

    reply_found = False
    expected_sender = normalize_email(record.recipient)

    for message in thread.get("messages", []):
        message_id = message.get("id", "")
        if message_id in known_sent_ids:
            continue

        sender = normalize_email(get_message_header(message, "From") or "")
        if sender == expected_sender:
            record.replied = True
            record.replied_at = parse_internal_date(message.get("internalDate"))
            reply_found = True
            break

    record.last_checked_at = utc_now_iso()
    return reply_found


def should_send_follow_up(record: EmailTrackingRecord, after_hours: int) -> bool:
    """Return True when recipient is eligible for follow-up email."""
    if record.replied or record.follow_up_sent:
        return False

    initial_sent_at = parse_iso_datetime(record.initial_sent_at)
    threshold = initial_sent_at + timedelta(hours=after_hours)
    now_utc = datetime.now(timezone.utc)
    return now_utc >= threshold


def send_follow_up_email(
    service: Resource,
    record: EmailTrackingRecord,
    subject: str,
    body: str,
) -> None:
    """Send follow-up email in the same thread and update tracking state."""
    raw_message = build_message(
        recipient=record.recipient,
        subject=subject,
        body=body,
        in_reply_to=record.initial_rfc822_message_id,
    )
    response = send_message(service, raw_message, thread_id=record.thread_id)
    record.follow_up_sent = True
    record.follow_up_message_id = response.get("id")
    record.follow_up_sent_at = utc_now_iso()


def safe_http_error_message(error: HttpError) -> str:
    """Convert HttpError into a concise display string."""
    return str(error)
