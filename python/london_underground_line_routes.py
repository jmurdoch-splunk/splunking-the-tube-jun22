
# encoding = utf-8

#
# Note:
# This particular AOB input was written to:
# - Determine valid London Underground routes
# - Provide enough metadata for each stop 
# - Make the data easy to work with (human)
# - Make the data fast to work with (splunk)
#
# It was *not* written to:
# - Reduce data volume (this can *increase* volume)
# - Show the best way to prepare a multi-dimensional data set
# - Showcase how to code
#
# Changes are welcome if they retain the values of the first 4 points
#

import os
import sys
import time
import datetime
import json

def validate_input(helper, definition):
    pass

def collect_events(helper, ew):
    # Get inputs
    opt_line_id = helper.get_arg('line_id')
    global_app_key = helper.get_global_setting("app_key")
    
    # Build Request
    url = "https://api.tfl.gov.uk/Line/" + opt_line_id + "/Route/Sequence/all"
    method = "GET"
    parameters = {}
    if global_app_key is None:
        parameters['app_key'] = global_app_key
    
    # Issue request
    response = helper.send_http_request(url, method, parameters=parameters, payload=None, headers=None, cookies=None, verify=True, cert=None, timeout=None, use_proxy=False)

    data = response.json()
    
    '''
    This Python takes Tube route data and makes sense of it.
    
    The problems with this data are:
    - it's multi-dimensional, held in different areas of the JSON
    - each route is split into reusable branch lines
    - station data is duplicated *multiple* times as branches can overlap
    - each official route is only defined by coordinates - not branch sequnences!
    
    My solution to this is:
    1) Compile a station list, noting coordinates and if a branchorigin
    2) Map the coordinates to the station list, building a route
    3) Concatenate metadata for each route step (1 stop per route = 1 event)
    '''
    #
    # Step 1: Build out station list
    #
    stationList = {}
    for seq in data['stopPointSequences']:
        # if there's no previous branches, this is the origin
        if not seq['prevBranchIds']:
            # if travelling out of here, what direction is it?
            direction = seq['direction']
        else:
            direction = ""

        # if it's the first entry, it's a origin
        origin = 1
        # for every stop...
        for station in seq['stopPoint']:
            # if it doesn't exist, add it
            if not station['id'] in stationList:
                stationList[station['id']] = { 
                    'name': station["name"], 
                    'gridRef': [station["lat"], station["lon"]],
                    'originDirection': ""
                }

            # if it's the first entry and has a direction, add that data
            if direction and origin:
                stationList[station['id']]['originDirection'] = direction
                origin = 0
    #
    # Step 2: Now build the routes, using the definitive coordinate list
    # 
    validRoutes = {}
    for ls in data['lineStrings']:
        route = []
        stationNum = 0
        
        # JSON encapsulated in JSON - no idea why they did this!
        line = json.loads(ls)
        # for each pair of coordinates...
        for stationCoords in line[0]:
            for key, value in stationList.items():
                # If we find the station, break and exit
                if [ stationCoords[1], stationCoords[0] ] == value['gridRef']:
                    stationData = {
                        'stationNum': stationNum,
                        'naptanId': key,
                        'stationName': value['name'],
                        'latitude': value['gridRef'][0],
                        'longitude': value['gridRef'][1],
                        'originDirection': value['originDirection']
                    }
                    break
            
            # if this is the first station, add the direction
            if stationNum == 0:
                direction = stationData['originDirection']
                stationData['prevNaptanId'] = ""
            else:
                # if not the first station, note the previous stop
                stationData['prevNaptanId'] = route[-1]['naptanId']

            stationData.pop('originDirection', None)
            route.append(stationData)
            
            stationNum += 1
     
        # now we have completed route, make a key "<ORIGIN>-<DESTINATION>"
        # this is very important for grouping in Splunk
        routekey = route[0]['naptanId'] + "-" + route[-1]['naptanId']

        # add the route dict
        validRoutes[routekey] = {}
        validRoutes[routekey]['stationSequence'] = route
        # add further textual metadata
        validRoutes[routekey]['direction'] = direction
        validRoutes[routekey]['originNaptanId'] = route[0]['naptanId']
        validRoutes[routekey]['destinationNaptanId'] = route[-1]['naptanId']
    
    #
    # Step 3: bring the overall metadata and station sequences together
    #
    for routeId in validRoutes:
        routeMeta = {
            'timestamp': time.time(),
            'routeId': routeId,
            'lineId': data['lineId'],
            'lineName': data['lineName'],
            'direction': validRoutes[routeId]['direction'],
            'destinationNaptanId': validRoutes[routeId]['destinationNaptanId'],
            'originNaptanId': validRoutes[routeId]['originNaptanId']
        }    
        # for each station on this route
        for station in validRoutes[routeId]['stationSequence']:
            payload = dict(routeMeta)
            payload.update(station)

            # write an event to splunk for each station on each route
            event = helper.new_event(source=helper.get_input_type(), index=helper.get_output_index(), sourcetype=helper.get_sourcetype(), data=json.dumps(payload))
            ew.write_event(event)
