
# encoding = utf-8

import os
import sys
import time
import datetime
import json

def validate_input(helper, definition):
    '''
    From /Line/Mode/<service>:
    
    tube: 
        bakerloo, central, circle, district, hammersmith-city, jubilee, 
        metropolitan, northern, piccadilly, victoria, waterloo-city
    
    elizabeth-line: 
        elizabeth
    '''
    
    pass

def collect_events(helper, ew):
    opt_lineids = helper.get_arg('lineids')
    global_app_key = helper.get_global_setting("app_key")

    url = "https://api.tfl.gov.uk/Line/" + opt_lineids + "/Arrivals"
    method = "GET"
    
    parameters = {}
    if global_app_key is None:
        parameters['app_key'] = global_app_key
    
    response = helper.send_http_request(url, method, parameters=parameters, payload=None, headers=None, cookies=None, verify=True, cert=None, timeout=None, use_proxy=False)
    response.raise_for_status()
    data = response.json()
    
    # Note - platform changes result in DUPLICATE data
    for arrival in data:
        output = { 
            # Essential data to ensure it's valid / appropriate
            "timestamp": arrival['timestamp'],
            "timeToLive": arrival['timeToLive'],
            "lineId": arrival['lineId'],
            # Vehicle detail
            "vehicleId": arrival['vehicleId'],
            "towards": arrival['towards'],
            # Arrival detail
            "naptanId": arrival['naptanId'], 
            "stationName": arrival['stationName'], 
            "platformName": arrival['platformName'], 
            "expectedArrival": arrival['expectedArrival'], 
            "timeToStation": arrival['timeToStation'],
            # These may be unknown if the train is stopped, cancelled, etc
            "direction": arrival.get('direction', "unknown"),
            "destinationNaptanId": arrival.get('destinationNaptanId', "unknown"),
            "destinationName": arrival.get('destinationName', "unknown")
        }
            
        event = helper.new_event(source=helper.get_input_type(), index=helper.get_output_index(), sourcetype=helper.get_sourcetype(), data=json.dumps(output))
        ew.write_event(event)
