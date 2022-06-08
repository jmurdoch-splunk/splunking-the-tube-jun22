
# encoding = utf-8

import os
import sys
import time
import datetime
import json
import xml.etree.ElementTree as ET

def validate_input(helper, definition):
    pass

def collect_events(helper, ew):
    # cap is 5000 req/hour
    opt_station = helper.get_arg('station_id')
    opt_rows = helper.get_arg('number_of_results')

    global_api_token = helper.get_global_setting("api_token")
    
    url = "https://lite.realtime.nationalrail.co.uk/OpenLDBWS/ldb6.asmx"
    method = "POST"
    headers = {
        'Content-Type': 'text/xml;charset=UTF-8'
    }
    soapxml = '''<?xml version="1.0"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="http://thalesgroup.com/RTTI/2014-02-20/ldb/" xmlns:ns2="http://thalesgroup.com/RTTI/2010-11-01/ldb/commontypes">
  <SOAP-ENV:Header>
    <ns2:AccessToken>
      <ns2:TokenValue>{token}</ns2:TokenValue>
    </ns2:AccessToken>
  </SOAP-ENV:Header>
  <SOAP-ENV:Body>
    <ns1:GetDepartureBoardRequest>
      <ns1:numRows>{rows}</ns1:numRows>
      <ns1:crs>{station}</ns1:crs>
    </ns1:GetDepartureBoardRequest>
  </SOAP-ENV:Body>
</SOAP-ENV:Envelope>'''.format(token=global_api_token, rows=opt_rows, station=opt_station)

    # The following examples send rest requests to some endpoint.
    response = helper.send_http_request(url, method, parameters=None, payload=soapxml, headers=headers, cookies=None, verify=True, cert=None, timeout=None, use_proxy=False)

    # get response status code
    # r_status = response.status_code
    # check the response status, if the status is not sucessful, raise requests.HTTPError
    # response.raise_for_status()
    
    xmlroot = ET.fromstring(response.text)
    ns = { 'x': 'http://thalesgroup.com/RTTI/2014-02-20/ldb/types' }
    timestamp = xmlroot.find('.//x:generatedAt', ns).text

    for svc in xmlroot.findall('.//x:service', ns):
        departure = {
            'time': timestamp,
            'crs': opt_station,
            'orig': svc.find('x:origin/x:location/x:locationName',ns).text,
            'dest': svc.find('x:destination/x:location/x:locationName', ns).text,
            'std': svc.find('x:std', ns).text,
            'etd': svc.find('x:etd', ns).text,
            'plat': "",
            'oper': svc.find('x:operator', ns).text 
        }

        if svc.find('x:platform', ns) is not None:
            departure['plat'] = svc.find('x:platform', ns).text

        event = helper.new_event(source=helper.get_input_type(), index=helper.get_output_index(), sourcetype=helper.get_sourcetype(), data=json.dumps(departure))
        ew.write_event(event)
