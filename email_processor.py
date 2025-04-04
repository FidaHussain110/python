import imaplib
import email
import smtplib
import time
import mysql.connector
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import re
import pytz

# Database connection
def connect_db():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="loadboard"
        )
        return conn
    except Exception as e:
        print(f"[❌] Database Error: {e}")
        return None

# Load authorized receivers
def load_receivers():
    try:
        with open("receivers.txt", "r") as file:
            receivers_list = [line.strip() for line in file.readlines() if line.strip()]
        print(f"[ℹ️] Loaded Receivers: {receivers_list}")
        return receivers_list
    except Exception as e:
        print(f"[❌] Error loading receivers: {e}")
        return []

# Load sender accounts
def load_senders():
    try:
        with open("senders.txt", "r") as file:
            senders_list = [tuple(line.strip().split(", ")) for line in file.readlines() if line.strip()]
        print(f"[ℹ️] Loaded Senders: {[s[0] for s in senders_list]}")
        return senders_list
    except Exception as e:
        print(f"[❌] Error loading senders: {e}")
        return []

receivers = load_receivers()
senders = load_senders()

sender_index = 0
send_limit = 500
send_count = {sender[0]: 0 for sender in senders}
processed_emails = set()
requests = {}  # Format: {(truck, origin_state): (to_email, original_message_id)}
sent_loads = {}  # Format: {original_message_id: set(shipmentIds)}

# Extract email details
def extract_request(body):
    request = {}
    patterns = {
        "Truck": r"Truck:\s*(.*)",
        "Origin": r"Origin:\s*(.*)",
        "Destination": r"Destination:\s*(.*)",
        "Pick Up Date": r"Pick Up Date:\s*([\d-]+)",
        "Drop Off Date": r"Drop Off Date:\s*([\d-]+)",
        "Full / Partial": r"Full / Partial:\s*(\w)",
        "Weight": r"Weight:\s*([\d,]+[\s\w]*)"
    }

    print(f"[📩] Extracting data from email:\n{body}\n")

    for key, pattern in patterns.items():
        match = re.search(pattern, body, re.IGNORECASE)
        if match:
            if key in ["Origin", "Destination"]:
                request[key] = (None, match.group(1).strip())
            else:
                request[key] = match.group(1).strip()
        else:
            print(f"[⚠️] Missing field: {key}")
            request[key] = None

    if not request["Truck"]:
        print("[❌] Truck type is required but not provided.")
        return None

    if not request["Origin"] and not request["Destination"]:
        print("[❌] Either Origin or Destination must be provided.")
        return None

    return request

# Fetch matching loads from load_details table
def fetch_loads(truck, origin_or_dest):
    conn = connect_db()
    if not conn:
        return None
    
    cursor = conn.cursor(dictionary=True)
    query = """
    SELECT ref_id, shipmentId, origin_city, origin_state, pickup_date, destination_city, destination_state, 
           drop_off_date, price, total_trip_mileage, full_partial, height AS weight, commodity, 
           truck_type, comments, company, phone, email, dot, docket, contact, website, timestamp 
    FROM load_details 
    WHERE truck_type LIKE %s 
    AND (origin_state LIKE %s OR destination_state LIKE %s)
    """
    params = [f"%{truck}%", f"%{origin_or_dest}%", f"%{origin_or_dest}%"]

    try:
        print(f"[🔍] Querying load_details with parameters: {params}")
        cursor.execute(query, tuple(params))
        loads = cursor.fetchall()
        if not loads:
            print("[ℹ️] No loads found matching truck_type and origin_state/destination_state.")
            return []

        current_time = datetime.now()
        recent_loads = [
            load for load in loads
            if (current_time - load['timestamp']).total_seconds() < 300
        ]
        if not recent_loads:
            print("[ℹ️] No loads found within 5 minutes in load_details.")
        else:
            print(f"[✅] Found {len(recent_loads)} loads within 5 minutes in load_details.")
        return recent_loads
    except Exception as e:
        print(f"[❌] Query Error: {e}")
        return None
    finally:
        cursor.close()
        conn.close()

# Format individual load response
def format_load_response(load, contact_email=None, contact_phone=None):
    ref_id = str(load.get('ref_id', 'N/A'))
    shipmentId = str(load.get('shipmentId', 'N/A'))
    origin_city = str(load.get('origin_city', 'N/A'))
    origin_state = str(load.get('origin_state', 'N/A'))
    destination_city = str(load.get('destination_city', 'N/A'))
    destination_state = str(load.get('destination_state', 'N/A'))
    pickup_hour = str(load.get('pick_up_hours', 'N/A'))
    drop_off_hours = str(load.get('drop_off_hours', 'N/A'))
    distance = str(load.get('total_trip_mileage', 'N/A'))
    weight = str(load.get('weight', 'N/A'))
    loadSize = str(load.get('full_partial', 'N/A'))
    equipment = str(load.get('truck_type', 'N/A'))
    broker = str(load.get('company', 'N/A'))
    phoneNum = str(load.get('phone', 'N/A'))
    email = str(load.get('email', 'N/A'))
    price = str(load.get('price', 'N/A'))
    comments = str(load.get('comments', 'N/A'))
    dot = str(load.get('dot', 'N/A'))
    docket = str(load.get('docket', 'N/A'))
    contact = str(load.get('contact', 'N/A'))
    website = str(load.get('website', 'N/A'))

    pickup_date = str(load.get('pickup_date', 'N/A'))
    dropoff_date = str(load.get('drop_off_date', 'N/A'))

    cst = pytz.timezone('America/Chicago')
    current_time_cst = datetime.now(cst)
    age_posted = current_time_cst.strftime("%I:%M %p (C.S.T.)").lstrip('0')

    if comments != 'N/A':
        comments_list = [comment.strip() for comment in comments.split('.') if comment.strip()]
        formatted_comments = []
        for comment in comments_list:
            if ':' in comment:
                key, value = [part.strip() for part in comment.split(':', 1)]
                formatted_line = f"🟢 {key}: {value} {'🟢' if value in ['N', 'Y'] else ''}"
            else:
                formatted_line = f"🟢 {comment}"
            formatted_comments.append(formatted_line)
        formatted_comments = '\n'.join(formatted_comments)
    else:
        formatted_comments = 'N/A'

    contact_parts = []
    if phoneNum != 'N/A': contact_parts.append(phoneNum)
    if email != 'N/A': contact_parts.append(email)
    if website != 'N/A': contact_parts.append(website)
    contact_info = " / ".join(contact_parts).rstrip(" / ") if contact_parts else contact

    D = []
    if docket != 'N/A': D.append(docket[2:] if docket.startswith('MC') else docket)
    if dot != 'N/A': D.append(dot)
    doc = " / ".join(D).rstrip(" / ") if D else dot

    emoji = "🟩" if email and phoneNum else "📧" if email else "☎️" if phoneNum else ""
    subject_distance = distance.replace(' mi', '') if distance != 'N/A' else 'N/A'
    
    pickup_date_short = 'N/A'
    if pickup_date != 'N/A':
        try:
            pickup_dt = datetime.strptime(pickup_date, '%m-%d-%Y')
            pickup_date_short = f"{pickup_dt.month}-{str(pickup_dt.year)[-2:]}".lstrip('0')
        except (ValueError, TypeError):
            pickup_date_short = pickup_date

    subject = (
        f"🟩 - Truck: {equipment} - Pickup: {pickup_date_short} / {origin_city}, {origin_state} - "
        f"Drop: {destination_state} - {subject_distance} Mi - {weight} - Ref #: {ref_id}"
    )

    body = f"""
Age Posted: {age_posted}

Truck Type: {equipment}

Origin: {origin_city}, {origin_state}
Pickup Date: {pickup_date}
Pick Up Hours: {pickup_hour}
 
Destination: {destination_city}, {destination_state}
Drop off date: {dropoff_date}
Drop Off Hours: {drop_off_hours}

Price: Bid
Trip: {subject_distance} Mi
Full / Partial: {loadSize}
Weight: {weight}

Comments: {formatted_comments}

Company: {broker}
Docket / D.O.T: {doc}
Contact: {contact_info}
"""
    return subject, body

# Format no matches found response
def format_no_matches_response(truck, state):
    cst = pytz.timezone('America/Chicago')
    current_time_cst = datetime.now(cst)
    timestamp = current_time_cst.strftime("%I:%M %p (C.S.T.)").lstrip('0')
    
    subject = f"❌ No Matching Loads Found - Truck: {truck} - State: {state}"
    body = f"""
Timestamp: {timestamp}

We couldn't find any matching loads for your request:
Truck Type: {truck}
State: {state}

We'll keep searching and notify you if any matching loads become available within the next 5 minutes.
"""
    return subject, body

# Send reply email with retry logic
def send_reply(to_email, body, sender_email, sender_password, original_message_id, subject, shipment_id=None):
    global sender_index, send_count
    max_retries = 3
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            msg = MIMEMultipart()
            msg["From"] = sender_email
            msg["To"] = to_email
            msg["Subject"] = subject
            if original_message_id:
                msg["In-Reply-To"] = original_message_id
                msg["References"] = original_message_id
            msg.attach(MIMEText(body, "plain"))

            server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, to_email, msg.as_string())
            server.quit()

            send_count[sender_email] += 1
            if send_count[sender_email] >= send_limit:
                sender_index = (sender_index + 1) % len(senders)
                print(f"[ℹ️] Switching to next sender: {senders[sender_index][0]}")
            
            print(f"[✅] Sent response to {to_email} from {sender_email} {'for Shipment ID ' + str(shipment_id) if shipment_id else 'with no matches'} as reply to {original_message_id}")
            return True
            
        except smtplib.SMTPAuthenticationError as e:
            print(f"[❌] Authentication Error: {e}. Check sender credentials in senders.txt (use App Password if 2FA is enabled).")
            return False
        except smtplib.SMTPException as e:
            print(f"[❌] SMTP Error on attempt {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            return False
        except Exception as e:
            print(f"[❌] Send Error: {e}")
            return False
    return False

# Check for new emails and process them
def check_email():
    global sender_index, send_count, requests, sent_loads
    if not senders:
        print("[❌] No senders available")
        return
    
    email_user, email_pass = senders[sender_index]
    
    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(email_user, email_pass)
        mail.select("inbox")

        since_date = datetime.today().strftime("%d-%b-%Y")
        status, messages = mail.search(None, f'(UNSEEN SINCE "{since_date}")')
        email_ids = messages[0].split()

        for num in email_ids:
            if num in processed_emails:
                continue

            status, msg_data = mail.fetch(num, "(RFC822)")
            for response_part in msg_data:
                if isinstance(response_part, tuple):
                    msg = email.message_from_bytes(response_part[1])
                    sender_email = email.utils.parseaddr(msg["From"])[1]
                    original_message_id = msg.get("Message-ID", "")
                    original_subject = msg.get("Subject", "Load Request")

                    print(f"[📧] Email received from: {sender_email}")

                    if sender_email not in receivers:
                        print(f"[⚠️] Unauthorized sender: {sender_email}")
                        continue
                    
                    body = ""
                    if msg.is_multipart():
                        for part in msg.walk():
                            if part.get_content_type() == "text/plain":
                                body = part.get_payload(decode=True).decode(errors="ignore")
                                break
                    else:
                        body = msg.get_payload(decode=True).decode(errors="ignore")
                    
                    request = extract_request(body)
                    if request:
                        print(f"[✅] Extracted Request: {request}")
                        truck = request["Truck"]
                        origin_or_dest = request["Origin"][1] if request["Origin"] else request["Destination"][1]
                        state_match = re.search(r'\b[A-Z]{2}\b', origin_or_dest)
                        state = state_match.group(0) if state_match else origin_or_dest
                        request_key = (truck, state)
                        requests[request_key] = (sender_email, original_message_id)

                        if original_message_id not in sent_loads:
                            sent_loads[original_message_id] = set()

                        loads = fetch_loads(truck, state)
                        if loads:
                            for load in loads:
                                shipment_id = str(load.get('shipmentId', 'N/A'))
                                if shipment_id not in sent_loads[original_message_id]:
                                    subject, response_body = format_load_response(load)
                                    if send_reply(sender_email, response_body, email_user, email_pass, 
                                                original_message_id, subject, shipment_id):
                                        sent_loads[original_message_id].add(shipment_id)
                        else:
                            subject, response_body = format_no_matches_response(truck, state)
                            send_reply(sender_email, response_body, email_user, email_pass, 
                                    original_message_id, subject)
                        processed_emails.add(num)
        
        mail.logout()
    except Exception as e:
        print(f"[❌] Email Error: {e}")

# Check database for new loads matching stored requests
def check_database():
    global sender_index, send_count, requests, sent_loads
    if not senders:
        print("[❌] No senders available")
        return
    
    email_user, email_pass = senders[sender_index]
    
    for (truck, state), (to_email, original_message_id) in list(requests.items()):
        if original_message_id not in sent_loads:
            sent_loads[original_message_id] = set()
        
        loads = fetch_loads(truck, state)
        if loads:
            for load in loads:
                shipment_id = str(load.get('shipmentId', 'N/A'))
                if shipment_id not in sent_loads[original_message_id]:
                    subject, response_body = format_load_response(load)
                    if send_reply(to_email, response_body, email_user, email_pass, 
                                original_message_id, subject, shipment_id):
                        sent_loads[original_message_id].add(shipment_id)
        else:
            subject, response_body = format_no_matches_response(truck, state)
            send_reply(to_email, response_body, email_user, email_pass, 
                    original_message_id, subject)

# Run continuously
while True:
    try:
        print("[🔄] Checking emails...")
        check_email()
        print("[🔍] Checking database for new loads...")
        check_database()
        print("[⏳] Waiting 5 seconds...")
        time.sleep(5)  # Increased from 1 to 5 seconds to reduce SMTP load
    except Exception as e:
        print(f"[❌] Main loop error: {e}")
        time.sleep(60)  # Wait longer if there's an error