
# encoding = utf-8

import os
import sys
import time
import datetime
import json

def validate_input(helper, definition):
    # TBC
    # - Validate naptanId
    # - Validate app_key
    # - Poll >= 300secs (crowding is updated every 5 mins)
    pass

def collect_events(helper, ew):
    # Bring in the station id (AKA Naptan)
    opt_naptan_id = helper.get_arg('naptan_id')
    global_app_key = helper.get_global_setting("app_key")
    
    # This ***MANDATES*** an API key
    parameters = {}
    if global_app_key is not "":
        parameters['app_key'] = global_app_key
    
    # We want timestamp and naptan to come first (these are missing too)    
    output = {
        'timestamp': ("%.6f" % time.time()),
        'naptanId': opt_naptan_id 
    }

    # Build Request to the crowding API
    url = 'https://api.tfl.gov.uk/crowding/' + opt_naptan_id + '/Live'
    method = 'GET'
    response = helper.send_http_request(url, method, parameters=parameters, payload=None, headers=None, cookies=None, verify=True, cert=None, timeout=None, use_proxy=True)
    
    # Append the received data to our known data
    output.update(response.json())
    
    # Write the event
    event = helper.new_event(source=helper.get_input_type(), index=helper.get_output_index(), sourcetype=helper.get_sourcetype(), data=json.dumps(output))
    ew.write_event(event)
