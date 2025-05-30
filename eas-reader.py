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
import os
import base64
import pyproj
import logging

# --- Logger Setup ---
logger = logging.getLogger(__name__)
if not logging.getLogger().hasHandlers():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        stream=sys.stderr
    )

# --- Global Variables & Constants ---
NO_WRITE = False
SOCKETIO_SERVERS = []
LOKI_URL = ""
LOKI_USERNAME = ""
LOKI_PASSWORD = ""
aliases = {}
utm_bounds = {}

message_buffer = deque(maxlen=20) 
shutdown_event = threading.Event()

frv_appliance_prefixes = {
    "P": "Pumper", "PT": "Pumper-Tanker", "UP": "Ultra Large Pumper",
    "LP": "Ladder Platform", "AP": "Aerial Pumper", "TB": "Teleboom",
    "T": "Transporter", "PP": "Pumper Platform", "CU": "Control Unit",
    "DC": "District Car", "R": "Rescue", "POD": "Pod", "HZ": "Hazardous Materials",
}
FRV_APPLIANCE_PREFIX_TUPLE = tuple(frv_appliance_prefixes.keys())


cfa_event_types = {
    "G&S": "Grass & Scrub", "RESC": "Rescue", "ALAR": "Alarm Panel",
    "INCI": "Incident", "NOST": "Non-Structure Fire",
    "NS&R": "Non-Structure Fire & Rescue", "STRU": "Structure Fire",
    "HIAR": "High Angle Rescue", "TRCH": "Trench Rescue",
    "CONF": "Confined Space Rescue", "STCO": "Structure Collapse",
    "MINE": "Mine Rescue",
}

frv_event_types = {
    "AAFIP": "Panel Alarm", "AAVMA": "Valve Alarm", "AASPR": "Sprinkler Alarm",
    "AAMCP": "Break Glass Alarm", "GS": "Grass & Scrub", "IN": "Incident",
    "NS": "Non-Structure Fire", "SF": "Structure Fire", "HZ": "Hazardous Materials",
    "MR": "Medical Response", "UN": "Unknown",
}

emr_event_types = {
    "AFEM": "EMR - AFEM", "AFPE": "EMR- AFPE", "AFPEM": "EMR - AFPEM",
}

# --- Pre-compiled Regex Patterns ---
RE_EAS_PRIORITY_EMERGENCY = re.compile(r"@@")
RE_EAS_PRIORITY_NON_EMERGENCY = re.compile(r"Hb")
RE_EAS_PRIORITY_ADMIN = re.compile(r"QD")
RE_ALERT = re.compile(r"ALERT:?")

RE_AREA_CFA = re.compile(r"[A-Z]{3,6}[0-9]{1,2}[A-Z]?") 
RE_AREA_FRV = re.compile(r"[0-9]{5}[A-Z]?")

CFA_EVENT_KEYS_PATTERN = "|".join(re.escape(k) for k in cfa_event_types.keys())
RE_CFA_EVENT_TYPE_CODE = re.compile(fr"(?P<event_type>{CFA_EVENT_KEYS_PATTERN})C(?P<code>\d)")

FRV_EVENT_KEYS_PATTERN = "|".join(re.escape(k) for k in frv_event_types.keys())
RE_FRV_EVENT_TYPE_CODE = re.compile(fr"(?P<event_type>{FRV_EVENT_KEYS_PATTERN})(?: )(?P<code>\dA)")

EMR_EVENT_KEYS_PATTERN = "|".join(re.escape(k) for k in emr_event_types.keys())
RE_EMR_EVENT_TYPE = re.compile(fr"({EMR_EVENT_KEYS_PATTERN})")


RE_FGD_CHANS = re.compile(r"(FGD)([0-9]{1,3})")
RE_JOB_IDS_PATTERN_TEXT = r"(?P<job_type>[FSEJ])(?P<job_num>[0-9]{9,11})" # Added J
RE_JOB_IDS = re.compile(RE_JOB_IDS_PATTERN_TEXT)
RE_PAGED_GROUP = re.compile(r"\s*(\[)([A-Z,0-9,_]{3,8})(\])")

RE_AGENCIES_RESOURCES = re.compile(
    r"(?:\*\s*(?P<advice>.+?)\s*\*\s*)?"
    r"(?P<agencies>(?:A|F|P|EM|R)+)?\s*"
    r"(?P<resources>.*?)"
    r"(?=(?:\s+(?:M|SV[A-Z]{1,2})\s+\S+\s+\S+\s+\([0-9]{6}\))|$)" 
)
RE_MAP_REF = re.compile(r"(?P<book>SV[A-Z]{1,2}|M)\s+(?P<map_num>\S+)\s+(?P<square>\S+)\s+\((?P<grid_ref>[0-9]{6})\)")
RE_EAS_LATLON_DMS = re.compile(r"LL\(([-+]?\d{1,3}):(\d{1,2}):([\d.]+),\s*([-+]?\d{1,3}):(\d{1,2}):([\d.]+)\)")
RE_EAS_LATLON_DECIMAL = re.compile(r"LAT/LON:([-+]?\d{1,3}(?:\.\d+)?),\s*([-+]?\d{1,3}(?:\.\d+)?)")

COMMON_WORDS_OVERLAPPING_FRV_PREFIXES = {"TO", "MOVE", "AT", "R"}


def load_aliases(csv_file="data/paging_aliases.csv"):
    lookup = {}
    try:
        with open(csv_file, "r", newline="", encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                key = row.get("short_name")
                if key:
                    lookup[key.upper()] = row 
    except FileNotFoundError:
        logger.error(f"Alias file not found: {csv_file}")
    except Exception as e:
        logger.error(f"Error loading aliases from {csv_file}: {e}")
    return lookup

def load_utm_bounds():
    utm_data = {'state': {}, 'melways': {}}
    try:
        with open("data/state_utm_bounds.csv", "r", newline="", encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                key = row.get("map_no")
                if key:
                    utm_data['state'][key.upper()] = row 
    except FileNotFoundError:
        logger.error("State UTM bounds file not found: data/state_utm_bounds.csv")
    except Exception as e:
        logger.error(f"Error loading state UTM bounds: {e}")
    try:
        with open("data/melways_utm_bounds.csv", "r", newline="", encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                key = row.get("map_no")
                if key:
                    utm_data['melways'][key.upper()] = row 
    except FileNotFoundError:
        logger.error("Melways UTM bounds file not found: data/melways_utm_bounds.csv")
    except Exception as e:
        logger.error(f"Error loading Melways UTM bounds: {e}")
    return utm_data

def send_to_loki(log_entry):
    if NO_WRITE:
        logger.info("No write mode enabled. Skipping sending log to Loki.")
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
            logger.debug("Log sent successfully to Loki.")
        else:
            logger.error(f"Failed to send log to Loki: {response.status_code} - {response.text}")
    except Exception:
        logger.exception(f"Error sending log to Loki")

def create_log_entry(message_data):
    timestamp_ns = str(round(time.time()) * 1000000000)
    stream_fields = message_data['labels'].copy()
    stream_fields['service_name'] = "EAS"
    for k, v in stream_fields.items():
        if not isinstance(v, str):
            stream_fields[k] = str(v)

    log_entry = {
        "streams": [{"stream": stream_fields, "values": [[timestamp_ns, json.dumps(message_data['message'])]]}]
    }
    logger.debug(json.dumps(log_entry, indent=4))
    return log_entry

def parse_message(data):
    parsed_message = {}
    labels = {}
    message_body_original_for_context = data['message'] 
    message_body = data['message'] 
    parsed_message['received_message'] = data['message']
    job_ids_to_collect = []


    priority_match = RE_EAS_PRIORITY_EMERGENCY.match(message_body)
    if priority_match: labels['eas_priority'] = "EMERGENCY"; message_body = message_body[priority_match.end():]
    elif (priority_match := RE_EAS_PRIORITY_NON_EMERGENCY.match(message_body)): labels['eas_priority'] = "NON_EMERGENCY"; message_body = message_body[priority_match.end():]
    elif (priority_match := RE_EAS_PRIORITY_ADMIN.match(message_body)): labels['eas_priority'] = "ADMIN"; message_body = message_body[priority_match.end():]
    else: labels['eas_priority'] = "UNKNOWN" 
    message_body = message_body.lstrip()

    capcode = int(data['address'])
    labels['capcode'] = capcode
    if capcode % 8 == 0: labels['address'] = capcode >> 3; parsed_message['address_range'] = "CFA"
    elif (capcode - 1) % 8 == 0: labels['address'] = (capcode - 1) >> 3; parsed_message['address_range'] = "SES"
    else: parsed_message['address_range'] = "UNKNOWN" 

    alert_match = RE_ALERT.match(message_body)
    if alert_match:
        parsed_message['alert'] = True
        message_body = message_body[alert_match.end():].lstrip()

    paged_group_match = RE_PAGED_GROUP.search(message_body_original_for_context) 
    if paged_group_match:
        paged_alias_raw = paged_group_match.group(2)
        paged_alias = paged_alias_raw.replace("_", "").upper() 
        paged_name, paged_district, paged_org = "Unknown", "Unknown", "Unknown"
        paged_lookup = aliases.get(paged_alias)
        if paged_lookup:
            paged_district = paged_lookup.get('district', "Unknown"); paged_org = paged_lookup.get('org', "Unknown"); paged_name = paged_lookup.get("name", "Unknown")
        elif (emr_alias_m := re.match(r"(E)([A-Z]{3,7})", paged_alias)):
            emr_base_alias = emr_alias_m.group(2)
            emr_lookup = aliases.get(emr_base_alias)
            if emr_lookup: paged_district = emr_lookup.get('district', "Unknown"); paged_org = 'EMR'; paged_name = f'{emr_lookup.get("name", "Unknown")} EMR'
        else:
            current_address_range = parsed_message.get('address_range')
            if current_address_range == "SES": paged_org, paged_name, paged_district = "SES", "Unknown SES", "Unknown SES"
            elif current_address_range == "CFA": paged_org, paged_name, paged_district = "Unknown Fire/EMR", "Unknown Fire/EMR", "Unknown Fire/EMR"

        parsed_message['paged'] = {'alias': paged_alias_raw, 'name': paged_name, 'district': paged_district, 'org': paged_org} 
        labels['paged_group'] = paged_alias_raw; labels['paged_group_district'] = paged_district; labels['paged_group_org'] = paged_org
        message_body = (message_body[:paged_group_match.start()] + message_body[paged_group_match.end():]).strip()

    temp_initial_job_ids = []
    leading_job_match = RE_JOB_IDS.match(message_body)
    if leading_job_match:
        job_id_val = leading_job_match.group(0)
        if job_id_val not in temp_initial_job_ids: temp_initial_job_ids.append(job_id_val)
        message_body = message_body[leading_job_match.end():].strip()
    job_ids_to_collect.extend(temp_initial_job_ids)

    if parsed_message.get('address_range') == "CFA":
        area_match_cfa = RE_AREA_CFA.match(message_body)
        if area_match_cfa: parsed_message['area'] = {'name': area_match_cfa.group(0), 'authority': "CFA"}; message_body = message_body[area_match_cfa.end():].lstrip()
        elif (area_match_frv := RE_AREA_FRV.match(message_body)): parsed_message['area'] = {'name': area_match_frv.group(0), 'authority': "FRV"}; message_body = message_body[area_match_frv.end():].lstrip()

        parsed_message.setdefault('event_type_code', {})
        cfa_event_match = RE_CFA_EVENT_TYPE_CODE.match(message_body)
        if cfa_event_match:
            parsed_message['event_type_code'] = cfa_event_match.groupdict()
            parsed_message['event_type_code']['event_type_name'] = cfa_event_types.get(cfa_event_match.group('event_type'), "")
            message_body = message_body[cfa_event_match.end():].lstrip()
        elif (frv_event_match := RE_FRV_EVENT_TYPE_CODE.match(message_body)):
            parsed_message['event_type_code'] = frv_event_match.groupdict()
            parsed_message['event_type_code']['event_type_name'] = frv_event_types.get(frv_event_match.group('event_type'), "")
            message_body = message_body[frv_event_match.end():].lstrip()
        elif (emr_event_match := RE_EMR_EVENT_TYPE.match(message_body)): 
            parsed_message.setdefault('event_type_code', {})['event_type'] = emr_event_match.group(1)
            message_body = message_body[emr_event_match.end():].lstrip()

    message_body, latlon = parse_eas_latlon(message_body)
    if latlon: parsed_message['latlon'] = latlon

    fgd_chans = []
    if parsed_message.get('address_range') == "CFA":
        temp_message_body_for_fgd = message_body
        fgd_iter = list(RE_FGD_CHANS.finditer(temp_message_body_for_fgd))
        for match in fgd_iter: fgd_chans.append(match.group(2))
        if fgd_chans:
            parsed_message['fgd_chans_list'] = sorted(list(set(fgd_chans)))
            message_body = RE_FGD_CHANS.sub("", message_body)
            message_body = " ".join(message_body.split()).strip() 

    if "MOVE TO STATION" in message_body_original_for_context.upper() and parsed_message.get('alert'):
        RE_MOVE_TO_STATION_SPECIFIC = re.compile(
            r"^(?P<moving_resources_text>(?:[A-Z0-9_/-]+\s+)*?)" 
            r"MOVE\s+TO\s+STATION\s+"
            r"(?P<target_station>[A-Z0-9_/-]+)\b", 
            re.IGNORECASE
        )
        move_match = RE_MOVE_TO_STATION_SPECIFIC.search(message_body)
        if move_match:
            parsed_message['move_up_details'] = {
                'resources_moving': [],
                'to_station': move_match.group("target_station").strip().upper()
            }
            moving_resources_str = move_match.group("moving_resources_text").strip()
            if moving_resources_str:
                units_moving = moving_resources_str.split()
                parsed_message.setdefault('resources_dict', {'FRV': []})
                for unit_raw in units_moving:
                    unit = unit_raw.upper()
                    if unit.startswith(FRV_APPLIANCE_PREFIX_TUPLE) and not (unit.startswith("FS") and unit[2:].isdigit()):
                         if unit not in parsed_message['resources_dict']['FRV']: parsed_message['resources_dict']['FRV'].append(unit)
                         if unit not in parsed_message['move_up_details']['resources_moving']:
                             parsed_message['move_up_details']['resources_moving'].append(unit)
                    elif re.fullmatch(r"T\d{1,2}[A-Z]?\b", unit): 
                         if unit not in parsed_message['resources_dict']['FRV']: parsed_message['resources_dict']['FRV'].append(unit)
                         if unit not in parsed_message['move_up_details']['resources_moving']:
                             parsed_message['move_up_details']['resources_moving'].append(unit)
            message_body = (message_body[:move_match.start()] + message_body[move_match.end():]).strip() if move_match else message_body

    map_ref_match = RE_MAP_REF.search(message_body)
    text_before_map_ref = message_body
    text_after_map_ref = ""

    if map_ref_match:
        parsed_message['map_ref'] = map_ref_match.groupdict()
        latlon_from_grid = grid_ref_to_latlon(parsed_message['map_ref'])
        if latlon_from_grid:
            parsed_message.setdefault('map_ref', {})['wgs84'] = {'latitude': latlon_from_grid[0], 'longitude': latlon_from_grid[1]}
        text_before_map_ref = message_body[:map_ref_match.start()]
        text_after_map_ref = message_body[map_ref_match.end():]
    
    text_for_resources_agencies = text_after_map_ref if map_ref_match else message_body
    agencies_resources_match = RE_AGENCIES_RESOURCES.match(text_for_resources_agencies.lstrip()) 
    
    if agencies_resources_match and parsed_message.get('address_range') == "CFA" and not parsed_message.get('move_up_details'):
        parsed_message.setdefault('resources_dict', {'CFA': [], 'FRV': [], 'EMR': [], 'air': [], 'other': []})
        resources_dict = parsed_message['resources_dict']

        advice_text = agencies_resources_match.group('advice')
        agencies_text = agencies_resources_match.group('agencies')
        resources_text_raw = agencies_resources_match.group('resources')

        if advice_text:
            parsed_message['advice'] = advice_text.replace("INFO:","\nINFO:").replace("RISK:","\nRISK:").replace("WARNING:","\nWARNING:").strip()

        if agencies_text:
            agency_list = parsed_message.get('agencies_list', [])
            agencies_stripped = agencies_text.strip().upper()
            for ag_char_code in ["A", "F", "P", "R"]: 
                if ag_char_code in agencies_stripped and ag_char_code not in agency_list:
                    agency_list.append(ag_char_code)
            if "EM" in agencies_stripped and "EM" not in agency_list : 
                agency_list.append("EM")
            if agency_list: parsed_message['agencies_list'] = sorted(list(set(agency_list)))

        if resources_text_raw:
            resources_items_raw = resources_text_raw.strip().split()
            remaining_tokens_after_resource_parse = []

            for r_item_raw_iter in resources_items_raw:
                r_item = r_item_raw_iter.upper() 
                classified = False
                alias_lookup_result = aliases.get(r_item)

                if RE_JOB_IDS.fullmatch(r_item_raw_iter):
                    if r_item_raw_iter not in job_ids_to_collect: job_ids_to_collect.append(r_item_raw_iter)
                    classified = True 
                elif r_item.startswith("LAT/LON:") or RE_EAS_LATLON_DMS.search(r_item_raw_iter) or RE_EAS_LATLON_DECIMAL.search(r_item_raw_iter) :
                    classified = True 

                if not classified and alias_lookup_result:
                    org = alias_lookup_result.get('org', '').upper()
                    if org == 'CFA' and r_item_raw_iter not in resources_dict['CFA']: resources_dict['CFA'].append(r_item_raw_iter); classified = True
                    elif org == 'FRV' and r_item_raw_iter not in resources_dict['FRV']: resources_dict['FRV'].append(r_item_raw_iter); classified = True
                    elif org == 'EMR' and r_item_raw_iter not in resources_dict['EMR']: resources_dict['EMR'].append(r_item_raw_iter); classified = True

                if not classified and re.fullmatch(r"T\d{1,2}[A-Z]?\b", r_item_raw_iter) and r_item_raw_iter not in resources_dict['FRV']: 
                    resources_dict['FRV'].append(r_item_raw_iter); classified = True

                if not classified:
                    m_cfa_tanker = re.fullmatch(r"([A-Z]{3,6})T(\d{1,2}[A-Z]?)", r_item_raw_iter)
                    m_c_prefix = re.fullmatch(r"C([A-Z]{3,6})", r_item_raw_iter)
                    if m_cfa_tanker and aliases.get(m_cfa_tanker.group(1).upper(), {}).get('org', '').upper() == 'CFA':
                        if r_item_raw_iter not in resources_dict['CFA']: resources_dict['CFA'].append(r_item_raw_iter); classified = True
                    elif m_c_prefix and aliases.get(m_c_prefix.group(1).upper(), {}).get('org', '').upper() == 'CFA':
                        if r_item_raw_iter not in resources_dict['CFA']: resources_dict['CFA'].append(r_item_raw_iter); classified = True
                    elif re.fullmatch(r"T\d{1,2}\b", r_item_raw_iter) and r_item_raw_iter not in resources_dict['FRV'] and r_item_raw_iter not in resources_dict['CFA']:
                        resources_dict['CFA'].append(r_item_raw_iter); classified = True

                if not classified and r_item_raw_iter.startswith('E') and len(r_item_raw_iter) > 1:
                    base_alias_emr = r_item_raw_iter[1:].upper()
                    alias_info_base_emr = aliases.get(base_alias_emr)
                    if alias_info_base_emr and alias_info_base_emr.get('org', '').upper() in ['EMR', 'CFA']:
                         if r_item_raw_iter not in resources_dict['EMR']: resources_dict['EMR'].append(r_item_raw_iter); classified = True

                if not classified and r_item_raw_iter.startswith(FRV_APPLIANCE_PREFIX_TUPLE) and \
                   r_item_raw_iter.upper() not in COMMON_WORDS_OVERLAPPING_FRV_PREFIXES and \
                   not (r_item_raw_iter.startswith("FS") and r_item_raw_iter[2:].isdigit()):
                    if r_item_raw_iter not in resources_dict['FRV']: resources_dict['FRV'].append(r_item_raw_iter); classified = True

                if not classified and r_item_raw_iter.startswith(("AIR","HEL","FBD")):
                    if r_item_raw_iter not in resources_dict['air']: resources_dict['air'].append(r_item_raw_iter); classified = True

                if not classified and r_item_raw_iter.startswith("EMR") and len(r_item_raw_iter) > 3: 
                    if r_item_raw_iter not in resources_dict['EMR']: resources_dict['EMR'].append(r_item_raw_iter); classified = True
                
                if not classified:
                    if RE_JOB_IDS.fullmatch(r_item_raw_iter):
                        if r_item_raw_iter not in job_ids_to_collect: job_ids_to_collect.append(r_item_raw_iter)
                    else: 
                        remaining_tokens_after_resource_parse.append(r_item_raw_iter)

            if map_ref_match:
                if agencies_resources_match:
                    message_body = (text_before_map_ref.strip() + " " + " ".join(remaining_tokens_after_resource_parse)).strip()
                else: 
                    message_body = text_before_map_ref.strip() 
            elif agencies_resources_match: 
                message_body = " ".join(remaining_tokens_after_resource_parse) 
            
    if 'resources_dict' in parsed_message:
        resources_dict_final_paged = parsed_message.get('resources_dict', {})
        paged_info_for_emr_final = parsed_message.get('paged')
        if paged_info_for_emr_final and paged_info_for_emr_final.get('org') == 'EMR':
            paged_alias_for_emr = paged_info_for_emr_final.get('alias','').upper()
            if paged_alias_for_emr.startswith('E') and len(paged_alias_for_emr) > 1:
                paged_base_alias_emr = paged_alias_for_emr[1:]
                items_to_move = []
                for cfa_res_item_emr in list(resources_dict_final_paged.get('CFA', [])):
                    cfa_res_base_emr = cfa_res_item_emr.upper()
                    if (m_c_pref_emr := re.fullmatch(r"C([A-Z]{3,6})", cfa_res_base_emr)): cfa_res_base_emr = m_c_pref_emr.group(1)
                    elif (m_tank_emr := re.fullmatch(r"([A-Z]{3,6})T(\d{1,2}[A-Z]?)", cfa_res_base_emr)): cfa_res_base_emr = m_tank_emr.group(1)
                    
                    if cfa_res_base_emr == paged_base_alias_emr:
                        items_to_move.append(cfa_res_item_emr)
                
                if items_to_move:
                    emr_list_final_paged = resources_dict_final_paged.setdefault('EMR', [])
                    for item_move_emr in items_to_move:
                        if item_move_emr in resources_dict_final_paged.get('CFA', []): resources_dict_final_paged['CFA'].remove(item_move_emr)
                        if item_move_emr not in emr_list_final_paged: emr_list_final_paged.append(item_move_emr)

    temp_general_job_ids_from_sub = []
    def collect_and_remove_job_ids_from_body(matchobj):
        job_id_val = matchobj.group(0)
        if job_id_val not in job_ids_to_collect: temp_general_job_ids_from_sub.append(job_id_val)
        return ""
    message_body = RE_JOB_IDS.sub(collect_and_remove_job_ids_from_body, message_body).strip()
    job_ids_to_collect.extend(temp_general_job_ids_from_sub)

    if job_ids_to_collect: parsed_message['job_ids_list'] = sorted(list(set(job_ids_to_collect)))

    parsed_message['message_body'] = " ".join(message_body.split()).strip()
    if parsed_message['message_body'].startswith("FROM ") and labels.get('eas_priority') == "ADMIN": 
        parsed_message['message_body'] = re.sub(r"^FROM\s+[A-Z0-9\s.-]+:\s*", "", parsed_message['message_body'], count=1, flags=re.IGNORECASE)

    if 'resources_dict' in parsed_message:
        for k_final in list(parsed_message['resources_dict'].keys()):
            if parsed_message['resources_dict'][k_final]:
                 parsed_message['resources_dict'][k_final] = sorted(list(set(parsed_message['resources_dict'][k_final])))
            else:
                del parsed_message['resources_dict'][k_final]
        if not parsed_message['resources_dict']:
            del parsed_message['resources_dict']
    if 'agencies_list' in parsed_message and parsed_message['agencies_list']:
        parsed_message['agencies_list'] = sorted(list(set(parsed_message['agencies_list'])))
    if 'fgd_chans_list' in parsed_message and parsed_message['fgd_chans_list']:
        parsed_message['fgd_chans_list'] = sorted(list(set(parsed_message['fgd_chans_list'])))

    if 'event_type_code' in parsed_message and not parsed_message['event_type_code']:
        del parsed_message['event_type_code']
    if 'advice' in parsed_message and parsed_message['advice']:
        parsed_message['advice'] = parsed_message['advice'].strip()

    message_data = {'message': parsed_message, 'labels': labels}
    return message_data


def dms_to_decimal(degrees, minutes, seconds):
    try:
        dec = abs(int(degrees)) + (int(minutes) / 60) + (float(seconds) / 3600)
        return -dec if int(degrees) < 0 else dec
    except ValueError:
        logger.error(f"Error converting DMS: deg={degrees}, min={minutes}, sec={seconds}")
        return 0.0

def parse_eas_latlon(message_body_input):
    latlon_coords = None
    message_body_output = message_body_input

    match_dms = RE_EAS_LATLON_DMS.search(message_body_input)
    if match_dms:
        try:
            lat_deg, lat_min, lat_sec, lon_deg, lon_min, lon_sec = match_dms.groups()
            latitude = dms_to_decimal(lat_deg, lat_min, lat_sec)
            longitude = dms_to_decimal(lon_deg, lon_min, lon_sec)
            message_body_output = (message_body_output[:match_dms.start()] + message_body_output[match_dms.end():]).strip()
            latlon_coords = {'latitude': latitude, 'longitude': longitude}
            return message_body_output, latlon_coords 
        except Exception:
            logger.exception(f"Error parsing DMS lat/lon from EAS message part: {match_dms.group(0)}")

    match_decimal = RE_EAS_LATLON_DECIMAL.search(message_body_output) 
    if match_decimal:
        try:
            lat_str, lon_str = match_decimal.groups()
            latitude = float(lat_str)
            longitude = float(lon_str)
            message_body_output = (message_body_output[:match_decimal.start()] + message_body_output[match_decimal.end():]).strip()
            latlon_coords = {'latitude': latitude, 'longitude': longitude}
        except Exception:
            logger.exception(f"Error parsing decimal lat/lon from EAS message part: {match_decimal.group(0)}")

    return message_body_output, latlon_coords


def expand_utm_grid_ref(grid_ref, map_x_min, map_x_max, map_y_min, map_y_max):
    try:
        x_6fig = int(grid_ref[:3])
        y_6fig = int(grid_ref[3:])
        x_6fig_metres = x_6fig * 100
        y_6fig_metres = y_6fig * 100
        k_x = next(k for k in range((map_x_min - x_6fig_metres) // 100000, (map_x_max - x_6fig_metres) // 100000 + 2)
                    if map_x_min <= 100000 * k + x_6fig_metres <= map_x_max)
        k_y = next(k for k in range((map_y_min - y_6fig_metres) // 100000, (map_y_max - y_6fig_metres) // 100000 + 2)
                    if map_y_min <= 100000 * k + y_6fig_metres <= map_y_max)
        x_map = 100000 * k_x + x_6fig_metres
        y_map = 100000 * k_y + y_6fig_metres
        return x_map, y_map
    except StopIteration:
        logger.debug(f"Could not expand grid ref {grid_ref} with bounds: X({map_x_min}-{map_x_max}), Y({map_y_min}-{map_y_max})")
        return None, None
    except Exception:
        logger.exception(f"Error in expand_utm_grid_ref for {grid_ref}")
        return None, None

def grid_ref_to_latlon(map_ref_data):
    try:
        book_raw = map_ref_data.get('book', '').upper()
        map_number_raw = map_ref_data.get('map_num', '').upper()
        grid_ref_str = map_ref_data.get('grid_ref')

        if not all([book_raw, map_number_raw, grid_ref_str]):
            logger.debug(f"Missing map_ref components for WGS84 conversion: {map_ref_data}")
            return None

        if book_raw == 'M': bounds_set = utm_bounds.get('melways', {})
        else: bounds_set = utm_bounds.get('state', {})

        mb = bounds_set.get(map_number_raw)
        if not mb:
            if map_number_raw[:-1].isdigit() and map_number_raw[-1].isalpha():
                mb = bounds_set.get(map_number_raw[:-1])
            if not mb:
                logger.debug(f"Map number {map_number_raw} (book {book_raw}) not found in UTM bounds.")
                return None

        required_keys = ['mga_zone', 'x_min', 'x_max', 'y_min', 'y_max']
        for key_check in required_keys:
            if key_check not in mb or not mb[key_check]:
                logger.debug(f"Missing or empty key '{key_check}' in UTM bounds for map {map_number_raw} (book {book_raw}). Data: {mb}")
                return None
        
        mga_zone = int(mb['mga_zone'])
        easting, northing = expand_utm_grid_ref(
            grid_ref_str, int(mb['x_min']), int(mb['x_max']),
            int(mb['y_min']), int(mb['y_max'])
        )
        if easting is None or northing is None: return None
        
        source_crs_epsg = 28300 + mga_zone
        
        gda_crs = pyproj.CRS.from_epsg(source_crs_epsg)
        wgs84_crs = pyproj.CRS.from_epsg(4326) 

        transformer = pyproj.Transformer.from_crs(gda_crs, wgs84_crs, always_xy=True)
        longitude, latitude = transformer.transform(easting, northing)
        return latitude, longitude
    except KeyError as e: logger.debug(f"Missing key in map_ref_data for WGS84: {e}. Data: {map_ref_data}"); return None
    except ValueError as e: logger.debug(f"ValueError during grid ref conversion for WGS84: {e}. Data: {map_ref_data}, Bounds: {mb if 'mb' in locals() else 'N/A'}"); return None
    except Exception: logger.exception(f"General error converting grid reference for WGS84 for {map_ref_data}"); return None


def start_client(server_url, shutdown_event_ref):
    client_logger = logging.getLogger(f"SocketIOClient.{server_url.split('/')[-1] if '/' in server_url else server_url}")
    sio_client = socketio.Client(logger=False, engineio_logger=False)

    @sio_client.event
    def connect(): client_logger.info(f"Connected to Socket.IO server at {server_url}")

    @sio_client.event
    def messagePost(data):
        try:
            message_content = data.get('message', ''); address = data.get('address', '')
            dedup_key = f"{message_content}|{address}"
            if dedup_key in message_buffer: client_logger.debug(f"Duplicate message detected from {server_url}. Discarding: {dedup_key[:100]}..."); return
            message_buffer.append(dedup_key)
            
            client_logger.debug(f"Received from {server_url}: CAPCODE={data.get('address')}, MSG='{data.get('message')}'")
            message_data = parse_message(data)
            
            if not message_data: client_logger.error(f"Failed to parse message: {data}"); return
            message_data['server_url'] = server_url 
            log_entry = create_log_entry(message_data)
            send_to_loki(log_entry)
        except Exception: client_logger.exception(f"Error processing message with data: {data} from {server_url}")

    try:
        sio_client.connect(server_url, socketio_path="socket.io", transports=["websocket"])
        while not shutdown_event_ref.is_set(): time.sleep(0.1) 
        sio_client.disconnect(); client_logger.info(f"Disconnected from {server_url}")
    except socketio.exceptions.ConnectionError as e: client_logger.error(f"Connection failed to {server_url}: {e}")
    except Exception: client_logger.exception(f"Error in client loop for {server_url}")


def main():
    global NO_WRITE, SOCKETIO_SERVERS, LOKI_URL, LOKI_USERNAME, LOKI_PASSWORD, aliases, utm_bounds
    parser = argparse.ArgumentParser(description="EAS to Loki logger.")
    parser.add_argument("--no_write", action="store_true", help="Disable writing logs to Loki.")
    parser.add_argument("-c", "--config", default="config.json", help="Path to configuration file (default: config.json)")
    parser.add_argument("--test", action="store_true", help="Enable test mode to read messages from test/test_messages.csv and output JSON.")
    parser.add_argument("--test_range", help="Specify a line range for test mode, e.g., '1-10' or '5'. CSV header is line 0.")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging.")
    args = parser.parse_args()

    if args.debug: logging.getLogger().setLevel(logging.DEBUG); logger.debug("Debug mode enabled.")

    if os.environ.get("BASE64_CONFIG"):
        logger.info("Using BASE64_CONFIG for configuration.")
        try: config = json.loads(base64.b64decode(os.environ["BASE64_CONFIG"]).decode("utf-8"))
        except Exception: logger.exception("Error decoding BASE64_CONFIG"); sys.exit(1)
    else:
        try:
            with open(args.config, "r", encoding='utf-8') as f: config = json.load(f)
        except FileNotFoundError: logger.error(f"Config file not found: {args.config}"); sys.exit(1)
        except json.JSONDecodeError as e: logger.error(f"Error decoding JSON from {args.config}: {e}"); sys.exit(1)
        except Exception: logger.exception(f"Error loading config from {args.config}"); sys.exit(1)

    NO_WRITE = args.no_write
    SOCKETIO_SERVERS = config.get("socketio_servers", [])
    LOKI_URL = config.get("loki_url"); LOKI_USERNAME = config.get("loki_username"); LOKI_PASSWORD = config.get("loki_password")

    if not args.test:
        critical_missing = [name for name, val in [("socketio_servers", SOCKETIO_SERVERS), ("loki_url", LOKI_URL),
                                                   ("loki_username", LOKI_USERNAME), ("loki_password", LOKI_PASSWORD)] if not val]
        if critical_missing: logger.error(f"Missing critical configuration: {', '.join(critical_missing)}."); sys.exit(1)

    aliases = load_aliases(); utm_bounds = load_utm_bounds()

    if args.test:
        logger.info("Running in test mode from test/test_messages.csv. Outputting JSON to stdout.")
        test_csv_file = "test/test_messages.csv"
        test_data_and_results = []
        start_line, end_line = 1, float('inf')

        if args.test_range:
            try:
                if '-' in args.test_range:
                    start_str, end_str = args.test_range.split('-')
                    start_line = int(start_str)
                    end_line = int(end_str) if end_str else float('inf')
                else:
                    start_line = int(args.test_range)
                    end_line = start_line
                if start_line < 1: start_line = 1
                logger.info(f"Test range specified: lines {start_line} to {end_line if end_line != float('inf') else 'end'}")
            except ValueError:
                logger.error("Invalid test_range format. Use 'start-end' or 'line_number'. Exiting.")
                sys.exit(1)

        try:
            # Ensure the directory exists
            test_dir = os.path.dirname(test_csv_file)
            if test_dir and not os.path.exists(test_dir): # Check if test_dir is not empty
                 os.makedirs(test_dir, exist_ok=True)

            with open(test_csv_file, "r", newline="", encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                header = next(reader, None)
                if not header or len(header) < 3:
                    logger.error(f"Malformed header in {test_csv_file}. Expected at least 3 columns. Found: {header}")
                    sys.exit(1)

                for current_line_num, row in enumerate(reader, 1): # Line numbers start from 1 for user
                    if current_line_num < start_line:
                        continue
                    if current_line_num > end_line:
                        break

                    if len(row) < 3:
                        logger.warning(f"Skipping malformed row {current_line_num} in {test_csv_file}: {row}")
                        continue

                    test_msg_data_input = {'address': row[1].strip(), 'message': row[2].strip()}
                    logger.info(f"Processing test message line {current_line_num}: CAPCODE={test_msg_data_input['address']}, MSG='{test_msg_data_input['message'][:100]}...'")
                    parsed_output = parse_message(test_msg_data_input)

                    test_data_and_results.append({
                        'input_capcode': test_msg_data_input['address'],
                        'input_message_string': test_msg_data_input['message'],
                        'parsed_result': parsed_output
                    })

            if test_data_and_results:
                print(json.dumps(test_data_and_results, indent=4))
            else:
                logger.info(f"No messages processed or found/valid in {test_csv_file} for the specified range.")
                print("[]") # Output empty JSON array if no results
        except FileNotFoundError:
            logger.error(f"Error: Test CSV file not found: {test_csv_file}")
            print("[]")
            sys.exit(1)
        except Exception:
            logger.exception(f"Error processing test messages from {test_csv_file}")
            print("[]")
            sys.exit(1)
        return

    threads = []
    for server_url in SOCKETIO_SERVERS:
        logger.info(f"Starting client for {server_url}")
        thread = threading.Thread(target=start_client, args=(server_url, shutdown_event), daemon=True, name=f"Client-{server_url}")
        threads.append(thread); thread.start()

    try:
        while not shutdown_event.is_set():
            if threads and not any(t.is_alive() for t in threads):
                logger.warning("All client threads have terminated unexpectedly. Shutting down.")
                break
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Shutting down.")
    finally:
        shutdown_event.set()
        logger.info("Waiting for client threads to terminate...")
        for thread in threads:
            if thread.is_alive():
                thread.join(timeout=5) # Give threads 5 seconds to close
            if thread.is_alive():
                 logger.warning(f"Thread {thread.name} did not terminate cleanly after timeout.")
        logger.info("All threads processed. Exiting.")

if __name__ == "__main__":
    main()