
# encoding = utf-8

import os
import sys
import time
import datetime
import json

def validate_input(helper, definition):
    '''
    Valid TfL-operated Modes:
        bus, cable-car, cycle-hire, dlr, elizabeth-line, overground
        replacement-bus, river-bus, river-tour. tflrail, tram, tube
        
    Other modes:
        coach, cycle, interchange-keep-sitting, interchange-secure, 
        national-rail, taxi, walking
    '''
    pass

def collect_events(helper, ew):
    opt_modes = helper.get_arg('modes')
    opt_detail = helper.get_arg('detail')
    
    global_app_key = helper.get_global_setting("app_key")

    url = "https://api.tfl.gov.uk/Line/Mode/" + opt_modes + "/Status"
    method = "GET"
    
    parameters = {}
    if global_app_key is None:
        parameters['app_key'] = global_app_key
    parameters['detail'] = str(bool(opt_detail)).lower()
    
    
    response = helper.send_http_request(url, method, parameters=parameters, payload=None, headers=None, cookies=None, verify=True, cert=None, timeout=None, use_proxy=False)
    response.raise_for_status
    data = response.json()
    
    for line in data:
        output = {
            'time': str(datetime.datetime.now()),
            'id': line['id'],
            'name': line['name'],
            'created': line['created'],
            'modified': line['modified'],
            'status': line['lineStatuses'][0]['statusSeverityDescription'],
            'reason': line['lineStatuses'][0].get('reason', '')
        }
            
        event = helper.new_event(source=helper.get_input_type(), index=helper.get_output_index(), sourcetype=helper.get_sourcetype(), data=json.dumps(output))
        ew.write_event(event)
