import base64
from base64 import b64encode
import csv
import sys
import time
import pymysql
import fbgraph_config as cfg
import urllib.request, urllib.parse, urllib.error # for url encoding
import urllib.request, urllib.error, urllib.parse # for sending requests

try:
    import json
except ImportError:
    import simplejson as json

class Mixpanel(object):

    def __init__(self, api_secret):
        self.api_secret = api_secret

    def request(self, format='json'):
        '''let's craft the http request'''
        data = None
        request_url = 'https://mixpanel.com/api/2.0/engage/?'
        request_url = request_url 
        byteAPISecret = bytes(self.api_secret + ':', "utf-8")
        encodedAsciiAPISecret =  b64encode(byteAPISecret).decode("ascii")
        headers = {'Authorization': 'Basic {encoded_secret}'.format(encoded_secret=encodedAsciiAPISecret)}
        request = urllib.request.Request(request_url, data, headers)
        response = urllib.request.urlopen(request, timeout=120)
        decoded_response =  (response.read().decode("utf-8"))
        return decoded_response

    def unicode_urlencode(self, params):
        ''' Convert stuff to json format and correctly handle unicode url parameters'''

        if isinstance(params, dict):
            params = list(params.items())
        for i, param in enumerate(params):
            if isinstance(param[1], list):
                params[i] = (param[0], json.dumps(param[1]),)

        result = urllib.parse.urlencode([(k, isinstance(v, str) and v.encode('utf-8') or v) for k, v in params])
        return result

    def get_and_write_results(self):
        response = api.request()
        '''parameters['session_id'] = json.loads(response)['session_id']
        parameters['page'] = 0'''
        #global_total = json.loads(response)['total']

        '''print("Session id is %s \n" % parameters['session_id'])'''
        #print("Here are the # of people %d" % global_total)

        self._page_results(response)
        #self.export_csv("people_export_" + str(int(time.time())) + ".csv", paged)

    def _page_results(self, response):

        fname = "people_export2" + ".txt"

        has_results = True

        while has_results < 11475:
            responser = json.loads(response)['results']

            has_results = has_results + len(responser)
            print(has_results)
            print("---------------------------------------------------")
            self._write_results(responser, fname)

            if has_results:
                response = api.request()
        

    def _write_results(self, results, fname):
       json_string = json.dumps(results)
       jdata = json.loads(json_string)
       count =0
       list1 = [] 
       conn = pymysql.connect(cfg.mysql['host'], user = cfg.mysql['user'], port = cfg.mysql['port'], passwd=cfg.mysql['password'], db = cfg.mysql['dbname'],use_unicode=True, charset="utf8")
       for value in jdata:
                list1.append(value['$distinct_id'])
                print(value['$distinct_id'])
                with open(fname, 'a') as f:
                    f.write(json.dumps(value) + '\n')
                    count=count+1
                distinct_id= None
                email = None
                first_name = None
                city = None
                country_code = None
                timezone = None
                userId = None
                University = None
                last_seen = None
                if '$distinct_id' in value:
                    distinct_id= value['$distinct_id']
                if '$email' in value['$properties']:
                    email = value['$properties']['$email']
                if '$first_name' in value['$properties']:
                    first_name = value['$properties']['$first_name']
                if '$city' in value['$properties']:
                    city = value['$properties']['$city']
                if '$country_code' in value['$properties']:
                    country_code = value['$properties']['$country_code']
                if '$timezone' in value['$properties']:
                    timezone = value['$properties']['$timezone']
                if '$userId' in value['$properties']:
                    userId = value['$properties']['$userId']
                if 'University' in value['$properties']:
                    University = value['$properties']['University']
                if '$last_seen' in value['$properties']:
                    last_seen = value['$properties']['$last_seen']
                try:
                    with conn.cursor() as cursor:
                        sql = "INSERT INTO `Mixpanel_User_Info`(distinct_id,email,first_name,city,country_code,timezone,userId,University,last_seen) VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s)"
                        cursor.execute(sql, (distinct_id,email,first_name,city,country_code,timezone,userId,University,last_seen))
                    conn.commit()
                except Exception as e:
                    print(e)
           
           
       print(count)
        
       



if __name__ == '__main__':
   api_secret = ''

   api = Mixpanel(
        api_secret=api_secret
    )
   api.get_and_write_results()