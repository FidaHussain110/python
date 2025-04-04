import requests
import asyncio
import platform
from playwright.async_api import async_playwright
from datetime import datetime
import mysql.connector
from mysql.connector import Error
import random
import string
import json
import xmltodict

# Login credentials and URLs
login_url = "https://login.123loadboard.com/?rd=https%3A%2F%2Fmembers.123loadboard.com%2F"
email = "pitepib489@birige.com"
password = "NetWork.com0"
links = [
    "https://members.123loadboard.com/loads/search/results/?m.t=Regular&m.lt=20&m.fs=all&m.sB.f=PickupDate&m.sB.dr=Ascending&S.o.t=States&S.o.s%5B0%5D=AL&S.o.s%5B1%5D=CT&S.o.s%5B2%5D=DE&S.o.s%5B3%5D=FL&S.o.s%5B4%5D=GA&S.o.s%5B5%5D=IN&S.o.s%5B6%5D=KY&S.o.s%5B7%5D=ME&S.o.s%5B8%5D=MD&S.o.s%5B9%5D=MA&S.o.s%5B10%5D=MI&S.o.s%5B11%5D=MS&S.o.s%5B12%5D=NH&S.o.s%5B13%5D=NJ&S.o.s%5B14%5D=NY&S.o.s%5B15%5D=NC&S.o.s%5B16%5D=OH&S.o.s%5B17%5D=PA&S.o.s%5B18%5D=RI&S.o.s%5B19%5D=SC&S.o.s%5B20%5D=TN&S.o.s%5B21%5D=VT&S.o.s%5B22%5D=VA&S.o.s%5B23%5D=WV&S.o.r=0&S.d.t=Anywhere&S.d.r=0&S.m.t=Regular&S.m.lt=20&S.m.fs=all&S.m.sB.dr=Ascending&S.m.sB.f=PostedDate&S.wL=true&S.wW=true&S.id=b0af97b8-3351-4afa-84b9-3748a73c6a84&id=b0af97b8-3351-4afa-84b9-3748a73c6a84",
    "https://members.123loadboard.com/loads/search/results/?m.t=Regular&m.lt=20&m.fs=all&m.sB.f=PostedDate&m.sB.dr=Ascending&S.o.s%5B0%5D=AK&S.o.s%5B1%5D=AZ&S.o.s%5B2%5D=AR&S.o.s%5B3%5D=CA&S.o.s%5B4%5D=CO&S.o.s%5B5%5D=ID&S.o.s%5B6%5D=IL&S.o.s%5B7%5D=IA&S.o.s%5B8%5D=KS&S.o.s%5B9%5D=LA&S.o.s%5B10%5D=MN&S.o.s%5B11%5D=MO&S.o.s%5B12%5D=MT&S.o.s%5B13%5D=NE&S.o.s%5B14%5D=NV&S.o.s%5B15%5D=NM&S.o.s%5B16%5D=ND&S.o.s%5B17%5D=OK&S.o.s%5B18%5D=OR&S.o.s%5B19%5D=SD&S.o.s%5B20%5D=TX&S.o.s%5B21%5D=UT&S.o.s%5B22%5D=WA&S.o.s%5B23%5D=WI&S.o.s%5B24%5D=WY&S.o.r=0&S.o.t=States&S.d.r=0&S.d.t=Anywhere&S.m.t=Regular&S.m.lt=20&S.m.fs=all&S.m.sB.f=PostedDate&S.m.sB.dr=Ascending&S.wL=true&S.wW=true&id=b0af97b8-3351-4afa-84b9-3748a73c6a84",
    "https://members.123loadboard.com/loads/search/results/?m.t=Regular&m.lt=20&m.fs=all&m.sB.f=PostedDate&m.sB.dr=Ascending&S.o.s%5B0%5D=AB&S.o.s%5B1%5D=BC&S.o.s%5B2%5D=MB&S.o.s%5B3%5D=NB&S.o.s%5B4%5D=NL&S.o.s%5B5%5D=NT&S.o.s%5B6%5D=NS&S.o.s%5B7%5D=NU&S.o.s%5B8%5D=ON&S.o.s%5B9%5D=PE&S.o.s%5B10%5D=QC&S.o.s%5B11%5D=SK&S.o.s%5B12%5D=YT&S.o.r=0&S.o.t=States&S.d.r=0&S.d.t=Anywhere&S.m.t=Regular&S.m.lt=20&S.m.fs=all&S.m.sB.f=PostedDate&S.m.sB.dr=Ascending&S.wL=true&S.wW=true&id=b0af97b8-3351-4afa-84b9-3748a73c6a84"
]

# MySQL configuration
MYSQL_CONFIG = {
    'host': 'sql110.infinityfree.com',
    'user': 'if0_37166458',
    'password': '2Xo4m4Q3fMsPnQJ',
    'database': 'if0_37166458_fida_db'
}

# Detect OS
OS_NAME = platform.system()

# Helper function to get timestamp for logs
def get_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Helper functions for parsing data
def parse_date(date_str):
    if not date_str:
        return "N/A"
    if isinstance(date_str, dict):
        date_str = date_str.get('#text', "N/A")
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
        return dt.strftime("%m-%d-%Y")
    except Exception as e:
        print(f"[{get_timestamp()}] Error parsing date '{date_str}': {e}")
        return "N/A"

def extract_text(value):
    """Extract #text from a dict or return the value as-is if it's not a dict."""
    if isinstance(value, dict):
        return value.get('#text', "N/A")
    return str(value) if value is not None else "N/A"

def extract_time(date_times, key="pickup"):
    print(f"[{get_timestamp()}] Extracting time for key '{key}', date_times: {date_times}")
    return "N/A"

def extract_comments(notes):
    return extract_text(notes)

def extract_website(website):
    return extract_text(website)

def extract_truck_types(equipments):
    print(f"[{get_timestamp()}] Extracting truck types from equipments: {equipments}")
    if equipments and isinstance(equipments, dict) and 'Equipment' in equipments:
        equipment = equipments['Equipment']
        if isinstance(equipment, list):
            return equipment[0].get("EquipmentType", "N/A") if equipment else "N/A"
        return equipment.get("EquipmentType", "N/A")
    return "N/A"

def format_price(price):
    print(f"[{get_timestamp()}] Formatting price: {price}")
    if isinstance(price, dict):
        if price.get('@i:nil') == 'true':
            return "N/A"
        amount = price.get("Amount", "N/A")
        type_ = price.get("Type", "N/A")
        return f"{amount} {type_}"
    return str(price) if price else "N/A"

def generate_unique_ref_id(cursor):
    characters = string.ascii_uppercase + string.digits
    max_attempts = 100
    for attempt in range(max_attempts):
        ref_id = ''.join(random.choices(characters, k=6))
        cursor.execute("SELECT ref_id FROM load_details WHERE ref_id = %s", (ref_id,))
        if not cursor.fetchone():
            print(f"[{get_timestamp()}] Generated unique ref_id: {ref_id}")
            return ref_id
        print(f"[{get_timestamp()}] Ref_id {ref_id} already exists, attempt {attempt + 1}/{max_attempts}")
    raise Exception(f"[{get_timestamp()}] Could not generate a unique ref_id after {max_attempts} attempts.")

def setup_database():
    try:
        print(f"[{get_timestamp()}] Connecting to MySQL host: {MYSQL_CONFIG['host']}")
        conn = mysql.connector.connect(
            host=MYSQL_CONFIG['host'],
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password']
        )
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES LIKE 'loadboard'")
        if cursor.fetchone():
            print(f"[{get_timestamp()}] Database 'loadboard' exists.")
        else:
            cursor.execute("CREATE DATABASE loadboard")
            print(f"[{get_timestamp()}] Database 'loadboard' created.")
        conn.close()

        print(f"[{get_timestamp()}] Reconnecting to database 'loadboard'")
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS load_details")
        print(f"[{get_timestamp()}] Existing 'load_details' table dropped.")

        create_table_sql = """
        CREATE TABLE load_details (
            ref_id VARCHAR(6) PRIMARY KEY,
            shipmentId VARCHAR(36) UNIQUE,
            origin_city VARCHAR(255),
            origin_state VARCHAR(255),
            pickup_date VARCHAR(10),
            pick_up_hours VARCHAR(255),
            destination_city VARCHAR(255),
            destination_state VARCHAR(255),
            drop_off_date VARCHAR(10),
            drop_off_hours VARCHAR(255),
            price VARCHAR(255),
            total_trip_mileage VARCHAR(255),
            full_partial VARCHAR(255),
            size VARCHAR(255),
            height VARCHAR(255),
            commodity VARCHAR(255),
            comments TEXT,
            company VARCHAR(255),
            dot VARCHAR(255),
            docket VARCHAR(255),
            contact VARCHAR(255),
            phone VARCHAR(255),
            fax VARCHAR(255),
            email VARCHAR(255),
            website VARCHAR(255),
            truck_type VARCHAR(255),
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_table_sql)
        print(f"[{get_timestamp()}] New 'load_details' table created with ref_id.")
        conn.commit()
        return conn, cursor
    except Error as e:
        print(f"[{get_timestamp()}] Error setting up database/table: {e}")
        return None, None

async def setup_browser_and_login(p):
    print(f"[{get_timestamp()}] Starting Playwright with WebKit on {OS_NAME}")
    browser = await p.webkit.launch(headless=False)
    page = await browser.new_page()
    print(f"[{get_timestamp()}] Browser launched, navigating to {login_url}")

    try:
        await page.goto(login_url, timeout=30000)
        print(f"[{get_timestamp()}] Waiting for email field")
        await page.wait_for_selector("#email", timeout=10000)
        await page.fill("#email", email)
        print(f"[{get_timestamp()}] Filled email field with {email}")
        await page.fill("#password", password)
        print(f"[{get_timestamp()}] Filled password field")
        await page.click("#sign-in-button")
        print(f"[{get_timestamp()}] Clicked sign-in button")
        await page.wait_for_selector("body", timeout=30000)
        current_url = page.url
        if "members.123loadboard.com" in current_url:
            print(f"[{get_timestamp()}] Login successful, current URL: {current_url}")
            return browser, page
        else:
            raise Exception("Login redirect did not reach members area")
    except Exception as e:
        print(f"[{get_timestamp()}] Login Error: {e}")
        print(f"[{get_timestamp()}] Check credentials or network connection")
        await browser.close()
        return None, None

async def get_session_cookies(page):
    cookies = await page.context.cookies()
    print(f"[{get_timestamp()}] Extracted cookies: {len(cookies)} cookies found")
    cookie_dict = {cookie['name']: cookie['value'] for cookie in cookies}
    print(f"[{get_timestamp()}] Cookies: {json.dumps(cookie_dict, indent=2)}")
    session = requests.Session()
    for cookie in cookies:
        session.cookies.set(cookie['name'], cookie['value'])
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/xml, text/xml, */*; q=0.01',
        'Referer': 'https://members.123loadboard.com/'
    })
    print(f"[{get_timestamp()}] Session cookies and headers set for requests")
    return session

async def capture_api_request(page, load_id):
    api_url = f"https://members.123loadboard.com/api/loads/{load_id}"
    request_headers = None
    
    async def on_request(request):
        nonlocal request_headers
        if api_url in request.url:
            request_headers = request.headers
            print(f"[{get_timestamp()}] Captured request headers for {load_id}: {json.dumps(request_headers, indent=2)}")
    
    page.on("request", on_request)
    
    print(f"[{get_timestamp()}] Navigating to API URL {api_url} to capture request")
    await page.goto(api_url, timeout=30000)
    await page.wait_for_load_state("networkidle", timeout=10000)
    
    return request_headers

def fetch_load_details(session, load_id, extra_headers=None):
    url = f"https://members.123loadboard.com/api/loads/{load_id}?fields=id,guid,status,computedMileage,age,created,poster,rateCheck,rateCheckPreview,metadata,postReference,numberOfLoads,originLocation,destinationLocation,pickupDateTime,pickupDateTimes,deliveryDateTime,equipments,loadSize,mileage,length,weight,rate,numberOfStops,commodity,notes,privateLoadNote,dispatchPhone,dispatchName,dispatchEmail,contactName,contactPhone,contactEmail,sortEquipCode,pricePerMile,teamDriving,conversation,extraStops,vendorInfo,vHub,rateNegotiations,canBookNow,bookNow,posterMetadata,onboardingUrl&onlineOnly=true"
    print(f"[{get_timestamp()}] Fetching load details for ID {load_id} from {url}")
    try:
        headers = session.headers.copy()
        if extra_headers:
            headers.update(extra_headers)
            print(f"[{get_timestamp()}] Using extra headers: {json.dumps(extra_headers, indent=2)}")
        
        response = session.get(url, headers=headers)
        content_type = response.headers.get('Content-Type', '').lower()
        print(f"[{get_timestamp()}] Response status: {response.status_code}")
        print(f"[{get_timestamp()}] Response content type: {content_type}")
        print(f"[{get_timestamp()}] Response headers: {json.dumps(dict(response.headers), indent=2)}")
        print(f"[{get_timestamp()}] Raw response text: {response.text[:200]}...")
        
        response.raise_for_status()
        
        if 'application/json' in content_type:
            print(f"[{get_timestamp()}] Successfully fetched load {load_id} as JSON, status: {response.status_code}")
            return response.json()
        elif 'application/xml' in content_type or 'text/xml' in content_type:
            print(f"[{get_timestamp()}] Successfully fetched load {load_id} as XML, status: {response.status_code}")
            xml_data = response.text
            return xmltodict.parse(xml_data).get('LoadDetails', {})
        else:
            print(f"[{get_timestamp()}] Unexpected content type: {content_type}")
            return None
    except Exception as e:
        print(f"[{get_timestamp()}] Error fetching load {load_id}: {e}")
        print(f"[{get_timestamp()}] Check session cookies or API endpoint")
        return None

def parse_load_json(data):
    print(f"[{get_timestamp()}] Parsing data: {json.dumps(data, indent=2)[:200]}...")
    try:
        poster = data.get("Poster", {})
        origin = data.get("OriginLocation", {}).get("Address", {})
        destination = data.get("DestinationLocation", {}).get("Address", {})
        equipments = data.get("Equipments", {})
        data_dict = {
            "shipmentId": extract_text(data.get("Id")),
            "origin_city": extract_text(origin.get("City")),
            "origin_state": extract_text(origin.get("State")),
            "pickup_date": parse_date(data.get("PickupDateTime")),
            "pick_up_hours": extract_time(data.get("PickupDateTimes", {}).get("dateTime", []), "pickup"),
            "drop_off_hours": extract_time(data.get("PickupDateTimes", {}).get("dateTime", []), "dropoff"),
            "destination_city": extract_text(destination.get("City")),
            "destination_state": extract_text(destination.get("State")),
            "drop_off_date": "N/A",  # Assuming no delivery date in current data
            "price": format_price(data.get("Rate")),
            "total_trip_mileage": extract_text(data.get("ComputedMileage")),
            "full_partial": extract_text(data.get("LoadSize")),
            "size": extract_text(data.get("Length")),
            "height": extract_text(data.get("Weight")),
            "commodity": extract_text(data.get("Commodity")),
            "comments": extract_comments(data.get("Notes")),
            "company": extract_text(poster.get("Name")),
            "dot": extract_text(poster.get("USDOTNumber")),
            "docket": extract_text(poster.get("BrokerMcNumber")),
            "contact": extract_text(data.get("DispatchName")),
            "phone": extract_text(data.get("DispatchPhone", {}).get("Number", "")),
            "fax": "N/A",
            "email": extract_text(data.get("DispatchEmail", "")),
            "website": extract_website(poster.get("WebSite")),
            "truck_type": extract_truck_types(equipments),
        }
        print(f"[{get_timestamp()}] Parsed load data: {data_dict}")
        return data_dict
    except Exception as e:
        print(f"[{get_timestamp()}] Error parsing data: {e}")
        print(f"[{get_timestamp()}] Check data structure or missing keys")
        return None

def insert_load_data(cursor, conn, load_data, ref_id):
    print(f"[{get_timestamp()}] Attempting to insert data for ref_id {ref_id}")
    try:
        cursor.execute("SELECT shipmentId FROM load_details WHERE shipmentId = %s", (load_data["shipmentId"],))
        if cursor.fetchone():
            print(f"[{get_timestamp()}] Shipment ID {load_data['shipmentId']} already exists, skipping")
            return

        insert_sql = """
        INSERT INTO load_details (
            ref_id, shipmentId, origin_city, origin_state, pickup_date, pick_up_hours,
            destination_city, destination_state, drop_off_date, drop_off_hours, price,
            total_trip_mileage, full_partial, size, height, commodity, comments, company,
            dot, docket, contact, phone, fax, email, website, truck_type
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (
            ref_id, load_data["shipmentId"], load_data["origin_city"], load_data["origin_state"],
            load_data["pickup_date"], load_data["pick_up_hours"], load_data["destination_city"],
            load_data["destination_state"], load_data["drop_off_date"], load_data["drop_off_hours"],
            load_data["price"], load_data["total_trip_mileage"], load_data["full_partial"],
            load_data["size"], load_data["height"], load_data["commodity"], load_data["comments"],
            load_data["company"], load_data["dot"], load_data["docket"], load_data["contact"],
            load_data["phone"], load_data["fax"], load_data["email"], load_data["website"],
            load_data["truck_type"]
        )
        cursor.execute(insert_sql, values)
        conn.commit()
        print(f"[{get_timestamp()}] Successfully inserted data for ref_id {ref_id}")
    except Error as e:
        print(f"[{get_timestamp()}] Error inserting data for ref_id {ref_id}: {e}")
        print(f"[{get_timestamp()}] Check database connection or table schema")

async def scrape_links_continuously(page, session, conn, cursor):
    while True:
        for link_num, link in enumerate(links, 1):
            try:
                load_ids = []
                print(f"[{get_timestamp()}] Link {link_num}: Setting up network response listener")
                async def handle_response(response):
                    if "ratecheck" in response.url.lower():
                        print(f"[{get_timestamp()}] Link {link_num}: Response from {response.url}")
                        text = await response.text()
                        rates = json.loads(text).get("rates", [])
                        load_ids.extend([rate.get("loadId") for rate in rates if rate.get("loadId")])
                        print(f"[{get_timestamp()}] Link {link_num}: Found load IDs: {load_ids}")

                page.on("response", handle_response)

                print(f"[{get_timestamp()}] Link {link_num}: Navigating to {link}")
                await page.goto(link, timeout=30000)
                print(f"[{get_timestamp()}] Link {link_num}: Waiting for page to load completely")
                await page.wait_for_load_state("networkidle", timeout=30000)

                print(f"[{get_timestamp()}] Link {link_num}: Processing {len(load_ids)} load IDs")
                for load_id in load_ids:
                    print(f"[{get_timestamp()}] Link {link_num}: Handling load ID {load_id}")
                    request_headers = await capture_api_request(page, load_id)
                    data = fetch_load_details(session, load_id, request_headers)
                    if data:
                        load_data = parse_load_json(data)
                        if load_data:
                            ref_id = generate_unique_ref_id(cursor)
                            insert_load_data(cursor, conn, load_data, ref_id)
                        else:
                            print(f"[{get_timestamp()}] Link {link_num}: Failed to parse data for load ID {load_id}")
                    else:
                        print(f"[{get_timestamp()}] Link {link_num}: No data returned for load ID {load_id}")
                    session = await get_session_cookies(page)
            except Exception as e:
                print(f"[{get_timestamp()}] Link {link_num}: Error during scraping: {e}")
                print(f"[{get_timestamp()}] Link {link_num}: Retrying in 60 seconds")
                await asyncio.sleep(60)

async def main():
    print(f"[{get_timestamp()}] Detected OS: {OS_NAME}")
    conn, cursor = setup_database()
    if not conn:
        print(f"[{get_timestamp()}] Database setup failed, exiting")
        return

    async with async_playwright() as p:
        browser, page = await setup_browser_and_login(p)
        if not browser or not page:
            print(f"[{get_timestamp()}] Failed to setup browser, exiting")
            conn.close()
            return

        session = await get_session_cookies(page)

        try:
            await scrape_links_continuously(page, session, conn, cursor)
        except KeyboardInterrupt:
            print(f"[{get_timestamp()}] Stopped by user")
        finally:
            print(f"[{get_timestamp()}] Closing browser and database connection")
            await browser.close()
            cursor.close()
            conn.close()
            print(f"[{get_timestamp()}] Resources closed.")

if __name__ == "__main__":
    asyncio.run(main())