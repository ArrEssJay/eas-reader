import json
import re
import sys
import requests
import socketio
import threading
from collections import deque
from requests.auth import HTTPBasicAuth
import argparse
import time
import csv
import os, base64, json, sys

# Global flag to control whether to write to Loki
NO_WRITE = False

# Load brigade lookup from CSV
def load_stations_lookup(csv_file="data/cfa_frv_stations.csv"):

    lookup = {}
    with open(csv_file, "r", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            key = row.get("short_name")
            if key:
                lookup[key] = row
    return lookup


# Global circular buffer for deduplication (max 10 items)
message_buffer = deque(maxlen=10)

shutdown_event = threading.Event()



# Map of event codes to descriptions
cfa_event_types = {
    "G&S": "Grass & Scrub",
    "RESC": "Rescue",
    "ALAR": "Alarm Panel",
    "INCI": "Incident",
    "NOST": "Non-Structure Fire",
    "NS&R": "Non-Structure Fire & Rescue",
    "STRU": "Structure Fire",
    "HIAR": "High Angle Rescue",
    "TRCH": "Trench Rescue",
    "CONF": "Confined Space Rescue",
    "STCO": "Structure Collapse",
    "MINE": "Mine Rescue"
}

frv_event_types = {
    "AAFIP": "Alarm Panel",
    "IN": "Incident",
    "NS": "Non-Structure Fire",
    "S": "Structure Fire",
    "HZ": "Hazardous Materials",
    "MR": "Medical Rescue",
}

def send_to_loki(log_entry):
    """Send the log entry to Loki with Basic Authentication,
       or skip if no_write mode is enabled."""
    if NO_WRITE:
        print("No write mode enabled. Skipping sending log to Loki.", file=sys.stderr)
        return
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(
            LOKI_URL,
            headers=headers,
            auth=HTTPBasicAuth(LOKI_USERNAME, LOKI_PASSWORD),
            data=json.dumps(log_entry)
        )
        if response.status_code == 204:
            print("Log sent successfully to Loki.", file=sys.stderr)
        else:
            print(f"Failed to send log to Loki: {response.status_code}", file=sys.stderr)
            print(response.text, file=sys.stderr)
    except Exception as e:
        print(f"Error sending log to Loki: {e}", file=sys.stderr)

def create_log_entry(parsed_message):
    """Create a log entry in Loki format."""

    #round the timestamp to the nearest second
    timestamp_ns = str(round(time.time()) * 1000000000)
    stream_fields = {"service_name": "EAS"}
    for key, value in parsed_message.items():
        if key != "received_message":
            stream_fields[key] = value
    log_entry = {
        "streams": [
            {
                "stream": stream_fields,
                "values": [
                    [timestamp_ns, parsed_message['received_message']]
                ]
            }
        ]
    }
    print(json.dumps(log_entry, indent=4))
    return log_entry

def parse_message(data):
    """Parse the EAS message and extract relevant fields."""
    parsed_message = {}
    message_body= data['message']
    # received message is sent as the log line. All other fields are sent as labels
    # max 15 labels, so we send data we don't care to filter on as json in 'message_body_data'
    # labels for table rendering, and another with structured json for more advanced rendering.
    # until downstream transformation can handle the structured data
    parsed_message['received_message'] = data['message']

    # Extract EAS priority
    if re.match(r"@@", message_body):
        parsed_message['eas_priority'] = "EMERGENCY"
    elif re.match(r"Hb", message_body):
        parsed_message['eas_priority'] = "NON_EMERGENCY"
    elif re.match(r"QD", message_body):
        parsed_message['eas_priority'] = "ADMIN"

    message_body = re.sub(r"@@|Hb|QD", "", message_body)

    # Extract address and address range
    capcode = int(data['address'])
    parsed_message['capcode'] = capcode

    if capcode % 8 == 0:
        parsed_message['address'] = capcode >> 3
        parsed_message['address_range'] = "CFA"
    elif  (capcode -1) % 8 == 0:
        parsed_message['address'] = (capcode-1) >> 3
        parsed_message['address_range'] = "SES"
    
    #Fire call matching is somewhat tricky as pages can be sent manually, and there may
    #be followup messages. Generally we're looking for something sent high priority with a job number
    #to a group capcode

    structured_message_body_data = {}
    message_body_data = {}

    # Alert
    alert = re.match(r"ALERT", message_body)
    if alert:
        parsed_message['alert'] = True
        message_body= message_body.replace(alert.group(), "").lstrip()

    # Area code will follow alert
    area = re.match(r"[A-Z]{4,6}[0-9]{1,2}[A-Z]{0,1}|[0-9]{5}[A-Z]?", message_body)
    if area:
        message_body= message_body.replace(area.group(), "").lstrip()
        parsed_message['area'] = area.group()
    
    # Extract event type and code
    cfa_event_type_code = re.match(
        r"(?P<event_type>G&S|RESC|ALAR|INCI|NOST|NS&R|STRU|TRCH|CONF|STCO|MINE|HIAR)C(?P<code>\d)", message_body
    )
    
    if cfa_event_type_code:
        message_body= message_body.replace(cfa_event_type_code.group(), "").lstrip()
        event_data = cfa_event_type_code.groupdict()
        event_data['event_type_long'] = cfa_event_types.get(event_data['event_type'], "")
        message_body_data['event_type_code'] = event_data
    
    # Otherwise check FRV event types/codes
    else:
        frv_event_type_code = re.match(r"(?P<event_type>AAFIP|IN|NS|HZ|S|MR)(?: )?(?P<code>\d[A-Z]?)", message_body)
        if frv_event_type_code:
            message_body= message_body.replace(frv_event_type_code.group(), "").lstrip()
            event_data = frv_event_type_code.groupdict()
            message_body_data['event_type_code'] = event_data


    # Extract Fireground channels
    fgd_chans = []
    fgd_chans_iter = list(set(re.finditer(r"(FGD)([0-9]{1,3})", message_body))) # Deduplicate list via set
    
    for match in fgd_chans_iter:
        fgd_chans.append(match.group(2))
        message_body= message_body.replace(match.group(), "").lstrip()
    
    structured_message_body_data['fgd_chans_list'] = fgd_chans
    message_body_data['fgd_chans'] =  ','.join(fgd_chans)  # Concatenate with comma


    # Extract ESTA job ID
    job_ids = re.finditer(r"(?P<job_type>F|S|E)(?P<job_num>[0-9]{9})", message_body)
    jobs = [j.group() for j in job_ids]

    if jobs:
        structured_message_body_data['job_ids_list'] = jobs
        message_body_data['job_ids'] =  ','.join(jobs)  # Concatenate with comma
        for j in jobs:
            message_body= message_body.replace(j, "").lstrip()


    # Extract paged group and lookup if present
    # CFA/FRV share an address space so we have to manually lookup
    paged_group = re.search(r"(\[)([A-Z,0-9,_]{4,6})(\])", message_body)
    if paged_group:
        message_body= message_body.replace(paged_group.group(), "").lstrip()
        # remove underscores from paged group
        parsed_message['paged_group'] = paged_group.group(2).replace("_", "")
        paged_lookup = brigades_lookup.get( paged_group.group(2))

        if paged_lookup:
            parsed_message['paged_group_district'] = paged_lookup.get('district', "")
            message_body_data['paged_group_name'] = paged_lookup.get("name", "")
            parsed_message['paged_group_org'] = paged_lookup.get("org", "")

    # use address range defaults if we don't know group details
    if 'paged_group_name' not in message_body_data.keys():
        if parsed_message['address_range'] == "SES":
            parsed_message['paged_group_org'] = "Unknown SES"
            message_body_data['paged_group_name'] = "Unknown SES"
            parsed_message['paged_group_district'] = "Unknown SES"
        
        elif parsed_message['address_range'] == "CFA":
            parsed_message['paged_group_org'] = "Unknown Fire"
            message_body_data['paged_group_name'] = "Unknown Fire"
            parsed_message['paged_group_district'] = "Unknown Fire"

    # Resources paged
    # After the map ref (XXXXXX) <agencies> <resources>
    # agencies is optional and not sent in soem resource request messages
    agencies_resources = re.search(r"(?<=\([0-9]{6}\))(?:.)(\* (?P<advice>.+) \*)?\s?(?P<agencies>(?:A|F|P|EM|R)+ )?(?P<resources>.*)", message_body)
    if agencies_resources:
    
        if agencies_resources.group('agencies'):
            message_body= message_body.replace(agencies_resources.group('agencies'), "").lstrip()
            agencies = agencies_resources.group('agencies').strip()
            agency_list = []
            if "A" in agencies:
                agency_list.append("A")
            if "F" in agencies:
                agency_list.append("F")
            if "P" in agencies:
                agency_list.append("P")
            if "EM" in agencies:
                agency_list.append("EM")
            if "R" in agencies:
                agency_list.append("R")
            structured_message_body_data['agencies_list'] = agency_list
            message_body_data['agencies'] =  ','.join(agency_list)  # Concatenate with comma
        
        if agencies_resources.group('resources'):
            message_body= message_body.replace(agencies_resources.group('resources'), "").lstrip()
            resources = agencies_resources.group('resources').split()

            resources_dict = {'CFA_BRIGADE': [], 'FRV': [], 'other': [] }
            resources_list = []

            for r in resources:
                #Also send a simple list for table rendering without transforming
                resources_list.append(r)
                if re.match(r"C[A-Z]{4}", r):
                    resources_dict['CFA_BRIGADE'].append(r[1:])
                elif r.startswith("P"):
                    resources_dict['FRV'].append(r)
                else:
                    resources_dict['other'].append(r)
                
            structured_message_body_data['resources_dict'] = resources_dict
            message_body_data['resources'] = ','.join(resources_list) # Concatenate with comma

        #advice
        if agencies_resources.group('advice'):
            message_body= message_body.replace(agencies_resources.group('advice'), "").lstrip()
            message_body_data['advice'] = agencies_resources.group('advice')

    # Extract book + page + grid refs
    location = re.search(r"(?P<book>SV[A-Z]{2}|M) (?P<page>\S+) (?P<square>\S+) \((?P<grid_ref>[0-9]{6})\)", message_body)
    if location:
        message_body= message_body.replace(location.group(), "").lstrip()
        structured_message_body_data['location_dict'] = location.groupdict()
        message_body_data['location'] = location.group()
   
    # Trim whitespace
    parsed_message['message_body'] = " ".join(message_body.split())

    # Add additional parsed data json
    parsed_message['message_body_data'] = json.dumps(message_body_data)
    parsed_message['structured_message_body_data'] = json.dumps(structured_message_body_data)

    return parsed_message

def start_client(server_url, shutdown_event):
    sio_client = socketio.Client(logger=False, engineio_logger=False)

    @sio_client.event
    def connect():
        print(f"Connected to Socket.IO server at {server_url}", file=sys.stderr)

    @sio_client.event
    def messagePost(data):
        """Handle incoming message with deduplication using message and address."""
        try:
            message = data['message']
            address = data['address']
            dedup_key = f"{message}|{address}"
            if dedup_key in message_buffer:
                print("Duplicate message detected. Discarding.", file=sys.stderr)
                return
            message_buffer.append(dedup_key)

            # print(f"Received event from {server_url} with data: {data}", file=sys.stderr)

            parsed_message = parse_message(data)
            parsed_message['server_url'] = server_url

            log_entry = create_log_entry(parsed_message)
            send_to_loki(log_entry)
        except Exception as e:
            print(f"Error processing message with data: {data} from {server_url}: {e}", file=sys.stderr)

    try:
        sio_client.connect(server_url, socketio_path="socket.io", transports=["websocket"])
        # Loop until shutdown_event is set
        while not shutdown_event.is_set():
            time.sleep(1)
        sio_client.disconnect()
    except Exception as e:
        print(f"Error connecting to {server_url}: {e}", file=sys.stderr)

def main():
    """Main execution: start a thread for each Socket.IO server and handle shutdown gracefully."""
    global NO_WRITE, SOCKETIO_SERVERS, LOKI_URL, LOKI_USERNAME, LOKI_PASSWORD, brigades_lookup

    parser = argparse.ArgumentParser(
        description="EAS to Loki logger with optional no-write mode and configurable config file."
    )
    parser.add_argument("--no_write", action="store_true",
                        help="Disable writing logs to Loki.")
    parser.add_argument("-c", "--config", default="config.json",
                        help="Path to configuration file (default: config.json)")
    parser.add_argument("--test", action="store_true",
                        help="Enable test mode to read messages from test_msgs.json.")
  
    args = parser.parse_args()

    # If the BASE64_CONFIG environment variable is set, decode and parse it as JSON.
    if os.environ.get("BASE64_CONFIG"):
        print("Using BASE64_CONFIG environment variable for configuration.", file=sys.stderr)
        try:
            config = json.loads(base64.b64decode(os.environ["BASE64_CONFIG"]).decode("utf-8"))
        except Exception as e:
            print(f"Error decoding BASE64_CONFIG: {e}", file=sys.stderr)
            sys.exit(1)
    else: 
        # load configuration from the specified config file
        config_file= args.config if args.config else "config.json"
        with open(config_file, "r") as f:
            config =  json.load(f)
    
    NO_WRITE = args.no_write
    SOCKETIO_SERVERS = config["socketio_servers"]
    LOKI_URL = config["loki_url"]
    LOKI_USERNAME = config["loki_username"]
    LOKI_PASSWORD = config["loki_password"]

    # Reload the brigade lookup in case paths or data have changed
    brigades_lookup = load_stations_lookup()

    if args.test:
        print("Running in test mode...", file=sys.stderr)
        try:
            with open("test/test_msgs.json", "r") as f:
                test_data = json.load(f)
                messages = test_data["messages"]
                processed_messages = []
                for msg in messages: 
                    processed_messages.append(parse_message(msg))
                #fieldnames =  set().union(*(d.keys() for d in processed_messages))
                fieldnames = [
                    'address', 'eas_priority', 'received_message', 'paged_group', 'paged_group_org', 'structured_message_body_data', 'message_body_data',
                              'paged_group_district', 'capcode', 
                                'address_range', 'alert',  'area', 'message_body'
                              ]
                writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(processed_messages)
                

        except FileNotFoundError:
            print("Error: test_msgs.json not found in the test directory.", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"Error processing test messages: {e}", file=sys.stderr)
            import traceback  # Import the traceback module
            traceback.print_exc()  # Print the full traceback
            sys.exit(1)
        return  # Exit after test mode


    threads = []
    for server_url in SOCKETIO_SERVERS:
        print(f"Starting client for {server_url}", file=sys.stderr)
        thread = threading.Thread(target=start_client, args=(server_url, shutdown_event), daemon=True)
        thread.start()
        threads.append(thread)

    try:
        # Keep the main thread alive while child threads run.
        while not shutdown_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        print("Received exit signal. Shutting down.", file=sys.stderr)
        shutdown_event.set()

    # Optionally join threads if needed.
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()
