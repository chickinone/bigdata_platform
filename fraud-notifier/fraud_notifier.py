import datetime
import json
import logging
import os
import smtplib
import ssl
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger('fraud-notifier')

TOPIC = 'fraud-alerts'

# Gmail SMTP config — load từ environment
SMTP_HOST = 'smtp.gmail.com'
SMTP_PORT = 587
EMAIL_FROM = os.getenv('EMAIL_FROM', '')
EMAIL_TO = os.getenv('EMAIL_TO', '')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD', '')

# Severity fallback (nếu message thiếu field severity)
SEVERITY_FALLBACK = {
    'VELOCITY_FRAUD': 'MEDIUM',
    'VELOCITY_ATTACK': 'HIGH',
    'FAILED_STORM': 'MEDIUM',
    'LARGE_AMOUNT': 'HIGH',
}

# Email throttle: max 1 email/account mỗi 5 phút
LAST_EMAIL_BY_ACCOUNT = {}
EMAIL_COOLDOWN_SEC = 300

# Severity nào sẽ gửi email
EMAIL_ON_SEVERITY = {'HIGH', 'MEDIUM'}

CLICKHOUSE_URL = 'http://clickhouse:8123/?user=admin&password=phantruong1'


def ms_to_datetime_str(ms):
    """Convert epoch milliseconds to readable UTC string."""
    if not ms:
        return 'N/A'
    try:
        return datetime.datetime.fromtimestamp(ms / 1000).strftime('%Y-%m-%d %H:%M:%S UTC')
    except Exception:
        return str(ms)


def esc_sql(s):
    """Escape single quotes for SQL string literal."""
    return str(s).replace("'", "\\'")


def format_console_alert(alert, severity):
    """Pretty print fraud alert with banner."""
    alert_type = alert.get('alert_type', 'UNKNOWN')
    account_id = alert.get('account_id', 'N/A')
    tx_count = alert.get('tx_count', 'N/A')
    threshold = alert.get('threshold', 'N/A')
    window_start = ms_to_datetime_str(alert.get('window_start_ms'))
    window_end = ms_to_datetime_str(alert.get('window_end_ms'))
    
    banner = '!' * 80 if severity == 'HIGH' else '=' * 80
    return (
        f"\n{banner}\n"
        f"🚨 FRAUD ALERT [{severity}]\n"
        f"  Alert type:   {alert_type}\n"
        f"  Account ID:   {account_id}\n"
        f"  Tx count:     {tx_count} (threshold: {threshold})\n"
        f"  Window:       {window_start} → {window_end}\n"
        f"{banner}\n"
    )


def send_email(alert, severity):
    """Send HTML email via Gmail SMTP. Throttled per account."""
    if not (EMAIL_FROM and EMAIL_TO and EMAIL_PASSWORD):
        log.warning('Email not configured (set EMAIL_FROM/TO/PASSWORD)')
        return False
    
    # Throttle per account
    account_id = str(alert.get('account_id', ''))
    now = time.time()
    last = LAST_EMAIL_BY_ACCOUNT.get(account_id, 0)
    if now - last < EMAIL_COOLDOWN_SEC:
        log.info(f'  → Email throttled for account {account_id} (cooldown)')
        return False
    LAST_EMAIL_BY_ACCOUNT[account_id] = now
    
    alert_type = alert.get('alert_type', 'UNKNOWN')
    tx_count = alert.get('tx_count', 'N/A')
    threshold = alert.get('threshold', 'N/A')
    window_start = ms_to_datetime_str(alert.get('window_start_ms'))
    window_end = ms_to_datetime_str(alert.get('window_end_ms'))
    
    subject = f'[{severity}] Fraud Alert: {alert_type} on Account {account_id}'
    color = {'HIGH': '#d32f2f', 'MEDIUM': '#f57c00', 'LOW': '#388e3c'}.get(severity, '#666')
    
    html_body = f"""
    <html>
      <body style="font-family: Arial, sans-serif;">
        <h2 style="color: {color};">🚨 Fraud Alert — {severity} Severity</h2>
        <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
          <tr style="background-color: #f5f5f5;">
            <td style="padding: 10px; border: 1px solid #ddd;"><b>Alert Type</b></td>
            <td style="padding: 10px; border: 1px solid #ddd;">{alert_type}</td>
          </tr>
          <tr>
            <td style="padding: 10px; border: 1px solid #ddd;"><b>Account ID</b></td>
            <td style="padding: 10px; border: 1px solid #ddd;">{account_id}</td>
          </tr>
          <tr style="background-color: #f5f5f5;">
            <td style="padding: 10px; border: 1px solid #ddd;"><b>Transaction Count</b></td>
            <td style="padding: 10px; border: 1px solid #ddd;">{tx_count} (threshold: {threshold})</td>
          </tr>
          <tr>
            <td style="padding: 10px; border: 1px solid #ddd;"><b>Window Start</b></td>
            <td style="padding: 10px; border: 1px solid #ddd;">{window_start}</td>
          </tr>
          <tr style="background-color: #f5f5f5;">
            <td style="padding: 10px; border: 1px solid #ddd;"><b>Window End</b></td>
            <td style="padding: 10px; border: 1px solid #ddd;">{window_end}</td>
          </tr>
        </table>
        <p style="margin-top: 20px; color: #666; font-size: 12px;">
          Automated alert from BigData Platform Fraud Detection system.<br>
          Investigate in Kibana: <a href="http://localhost:5601">http://localhost:5601</a>
        </p>
      </body>
    </html>
    """
    
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = EMAIL_FROM
    msg['To'] = EMAIL_TO
    msg.attach(MIMEText(html_body, 'html'))
    
    try:
        context = ssl.create_default_context()
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as server:
            server.starttls(context=context)
            server.login(EMAIL_FROM, EMAIL_PASSWORD)
            server.sendmail(EMAIL_FROM, EMAIL_TO.split(','), msg.as_string())
        log.info(f'  → Email sent to {EMAIL_TO} for account {account_id}')
        return True
    except Exception as e:
        log.error(f'  → Email failed: {e}')
        return False


def write_to_clickhouse(alert, severity, action):
    """Log notification event to ClickHouse for dashboard."""
    description = (
        f"Account {alert.get('account_id')}: {alert.get('tx_count')} tx "
        f"(threshold {alert.get('threshold')}) in window"
    )
    
    sql = (
        "INSERT INTO metrics.notification_events "
        "(rule_type, account_id, severity, action, description) "
        f"VALUES ('{esc_sql(alert.get('alert_type', ''))}', '{esc_sql(alert.get('account_id', ''))}', "
        f"'{esc_sql(severity)}', '{esc_sql(action)}', '{esc_sql(description)}')"
    )
    try:
        url = CLICKHOUSE_URL + '&query=' + urllib.parse.quote(sql)
        urllib.request.urlopen(
            urllib.request.Request(url, method='POST'), timeout=5
        ).read()
    except Exception as e:
        log.error(f'ClickHouse write failed: {e}')


def connect_with_retry(max_attempts=10):
    """Kafka có thể chưa ready khi container start."""
    for attempt in range(max_attempts):
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers='kafka:9092',
                group_id='fraud-notifier-v2',
                auto_offset_reset='earliest',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None,
                enable_auto_commit=True,
            )
            log.info('Connected to Kafka')
            return consumer
        except NoBrokersAvailable:
            log.warning(f'Kafka not ready, retry {attempt + 1}/{max_attempts}...')
            time.sleep(5)
    raise RuntimeError('Cannot connect to Kafka after retries')


def main():
    consumer = connect_with_retry()
    stats = defaultdict(int)
    last_stats = time.time()
    
    log.info(f'Fraud notifier started, monitoring "{TOPIC}"')
    log.info(
        f'Email config: from={EMAIL_FROM or "<not set>"}, '
        f'to={EMAIL_TO or "<not set>"}, '
        f'configured={bool(EMAIL_PASSWORD)}, '
        f'severity_filter={sorted(EMAIL_ON_SEVERITY)}'
    )
    
    for msg in consumer:
        try:
            alert = msg.value
            if not alert:
                continue
            
            alert_type = alert.get('alert_type', 'UNKNOWN')
            # Severity từ message, fallback nếu thiếu
            severity = alert.get('severity') or SEVERITY_FALLBACK.get(alert_type, 'LOW')
            
            stats[f'{alert_type}|{severity}'] += 1
            stats[f'TOTAL|{severity}'] += 1
            
            # Action 1: Console banner
            print(format_console_alert(alert, severity), flush=True)
            
            # Action 2: Email (chỉ HIGH/MEDIUM)
            email_sent = False
            if severity in EMAIL_ON_SEVERITY:
                email_sent = send_email(alert, severity)
            
            # Action 3: ClickHouse log
            action = 'EMAIL_SENT' if email_sent else ('EMAIL_THROTTLED' if severity in EMAIL_ON_SEVERITY else 'LOGGED_ONLY')
            write_to_clickhouse(alert, severity, action)
            
            log.info(
                f'Alert: type={alert_type}, severity={severity}, '
                f'account={alert.get("account_id")}, action={action}'
            )
            
            # Stats every 60s
            if time.time() - last_stats > 60:
                log.info('=== Notification Stats ===')
                for key in sorted(stats.keys()):
                    log.info(f'  {key}: {stats[key]}')
                log.info('==========================')
                last_stats = time.time()
        
        except Exception as e:
            log.error(f'Error processing alert: {e}')


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        log.info('Shutting down...')