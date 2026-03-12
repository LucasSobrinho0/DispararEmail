from __future__ import annotations

from pathlib import Path

from googleapiclient.errors import HttpError

from time import sleep

from random import randint

from email_campaign_core import (
    GMAIL_SEND_SCOPE,
    get_gmail_service,
    load_recipients,
    load_tracking_state,
    normalize_email,
    safe_http_error_message,
    save_tracking_state,
    send_initial_email,
)

CREDENTIALS_FILE = Path("credentials.json")
TOKEN_FILE = Path("token.json")
RECIPIENTS_CSV_FILE = Path("emails.csv")
TRACKING_FILE = Path("email_tracking_state.json")

INITIAL_EMAIL_SUBJECT = "SUA EMPRESA ESTÁ PERDENDO DINHEIRO EM TELECOM? VEJA COMO IDENTIFICAR E RESOLVER ISSO"
INITIAL_EMAIL_BODY = """
Oi, bom dia, tudo bem?

Sou Ana e trabalho na Alow.

A plataforma mostra onde empresas estão perdendo dinheiro em telecom, como linhas paradas, internet ociosa ou cobranças indevidas nas faturas.

Além de identificar, o sistema também faz a contestação direto na operadora e nosso time acompanha todo o processo.

Faz sentido te mostrar em 20 minutos como isso aparece na prática?

Assinado: Ana Paula, SDR do Alow
"""

INITIAL_EMAIL_CC = ["hello-98@alow.hs-inbox.com"]


def main() -> None:
    """Send initial emails and persist metadata needed for reply tracking."""
    try:
        gmail_service = get_gmail_service(
            CREDENTIALS_FILE,
            TOKEN_FILE,
            scopes=[GMAIL_SEND_SCOPE],
        )
        recipients = load_recipients(RECIPIENTS_CSV_FILE)
        tracking_records = load_tracking_state(TRACKING_FILE)

        for recipient in recipients:
            key = normalize_email(recipient)
            if key in tracking_records:
                print(f"Skipping {recipient}: already tracked.")
                continue

            try:
                record = send_initial_email(
                    service=gmail_service,
                    recipient=recipient,
                    subject=INITIAL_EMAIL_SUBJECT,
                    body=INITIAL_EMAIL_BODY,
                    cc_recipients=INITIAL_EMAIL_CC,
                )
                tracking_records[key] = record
                print(
                    f"Initial email sent to {recipient} "
                    f"(message id: {record.initial_message_id}, thread id: {record.thread_id})."
                )
                number = randint(30, 60)
                sleep(number)
            except HttpError as error:
                print(f"Failed to send to {recipient}: {safe_http_error_message(error)}")
                

        save_tracking_state(TRACKING_FILE, tracking_records)
        print(f"Tracking file updated: {TRACKING_FILE.resolve()}")
        
    except FileNotFoundError as error:
        print(f"File not found: {error.filename}")
    except ValueError as error:
        print(f"Invalid input: {error}")
    except Exception as error:  # pragma: no cover
        print(f"Unexpected error: {error}")


if __name__ == "__main__":
    main()
