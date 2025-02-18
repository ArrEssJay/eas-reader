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

# Global flag to control whether to write to Loki
NO_WRITE = False

# Load configuration from config.json
def load_config(config_file="config.json"):
    with open(config_file, "r") as f:
        return json.load(f)

# Load brigade lookup from CSV
def load_brigades_lookup(csv_file="data/cfa_brigades.csv"):
    """
    Load brigades lookup from a CSV file with columns:
    distrct_no, brig_no, brig_name, short_name, grp_name.
    Uses 'short_name' as the key.
    """
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
event_descriptions = {
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
        if key != "message":
            stream_fields[key] = value
    log_entry = {
        "streams": [
            {
                "stream": stream_fields,
                "values": [
                    [timestamp_ns, parsed_message['message']]
                ]
            }
        ]
    }
    print(json.dumps(log_entry, indent=4))
    return log_entry

def parse_message(data):
    """Parse the EAS message and extract relevant fields."""
    parsed_message = {}
    message = data['message']
    parsed_message['message'] = message

    # Extract EAS priority
    if re.match(r"@@", message):
        parsed_message['eas_priority'] = "EMERGENCY"
        parsed_message['message'] = re.sub(r"@@", "", message)
    elif re.match(r"Hb", message):
        parsed_message['eas_priority'] = "NON_EMERGENCY"
        parsed_message['message'] = re.sub(r"Hb", "", message)
    elif re.match(r"QD", message):
        parsed_message['eas_priority'] = "ADMIN"
        parsed_message['message'] = re.sub(r"QD", "", message)

    # Extract address and address range
    capcode = int(data['address'])
    parsed_message['capcode'] = data['address']

    if capcode % 8 == 0:
        parsed_message['address'] = capcode >> 3
        parsed_message['address_range'] = "CFA"
        parse_cfa_message(parsed_message)
    elif  (capcode -1) % 8 == 0:
        parsed_message['address'] = (capcode-1) >> 3
        parsed_message['address_range'] = "SES"
    else:
        parsed_message['address'] = -1
        parsed_message['address_range'] = "Unknown"
    
    return parsed_message


    

def parse_cfa_message(parsed_message):

    #Fire call
    alert = re.search(r"ALERT ", parsed_message['message'])
    if alert:
        parsed_message['f_alert'] = True

        #Area 
        f_area = re.search(r"[A-Z]{4,6}[0-9]{1,2}[A-Z]{0,1}|[0-9]{5}[A-Z]?", parsed_message['message'])
        if f_area:
         parsed_message['f_area'] = f_area.group()
    
        # Extract event type and code
        event_type_match = re.search(
            r"(G&S|RESC|ALAR|INCI|NOST|NS&R|STRU|TRCH|CONF|STCO|MINE|HIAR)C(\d+)",
            parsed_message['message']
        )
        if event_type_match:
            event_type_coded = event_type_match.group(1)
        
        parsed_message['f_event_type'] = event_descriptions.get(event_type_coded, "Unknown")
        parsed_message['f_code'] =  event_type_match.group(2)

        # Extract Fireground channels
        chans = re.findall(r"(?<=FGD)[0-9]{1,3}", parsed_message['message'])
        # Remove duplicates and sort
        if chans:
            parsed_message['f_fgd_chans'] = ", ".join(dict.fromkeys(chans)) 


        # Extract ESTA job ID
        job_id_match = re.search(r"F[0-9]{9}", parsed_message['message'])
        if job_id_match:
            parsed_message['f_job_id'] = job_id_match.group()



    # Extract paged group and lookup if present
    paged_group = re.search(r"(\[)([A-Z,0-9,_]{4,6})(\])", parsed_message['message'])
    if paged_group:
        parsed_message['f_paged'] = paged_group.group(2)
        brigade = brigades_lookup.get(parsed_message['f_paged'])
        if brigade:
            parsed_message['f_distrct_no'] = brigade.get("distrct_no", "Unknown")
            parsed_message['f_brig_name'] = brigade.get("brig_name", "Unknown")
        else:
            parsed_message['f_distrct_no'] = "Unknown"
            parsed_message['f_brig_name'] = "Unknown"
    else:
        parsed_message['f_paged'] = "Unknown"

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
    args = parser.parse_args()
    NO_WRITE = args.no_write

    # Reload configuration from the specified config file
    config = load_config(args.config)
    SOCKETIO_SERVERS = config["socketio_servers"]
    LOKI_URL = config["loki_url"]
    LOKI_USERNAME = config["loki_username"]
    LOKI_PASSWORD = config["loki_password"]

    # Reload the brigade lookup in case paths or data have changed
    brigades_lookup = load_brigades_lookup()

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
