import json
import requests
import pandas as pd
import datetime

TIMEOUT = 60
SATALIA_URL = 'https://pr.dragonfly.delivery.satalia.io/core/v2'
with open('/home/jupyter/sataliakey.txt', 'r') as f:
    SATALIA_KEY = f.read()

class SataliaBase:
    def __init__(self, key=None, url = None, timeout=30):
        
        self.url = url

        self.header = {'Authorization': key,
                       'Content-Type': 'application/json'}
        self.timeout = timeout

        self._status_ok = True
        
    def get_schedule(self, depot_id, params):
        
        response = requests.get(
                url=f'''{self.url}/schedules/{depot_id}''',
                params=params,
                headers=self.header,
            timeout=self.timeout)
        return response.json()

def parse_schedule(data):
    ords = []
    trs = [] ##check if data i is duplicates? ##create a date
    for d in data:
        t = d['metadata']['timeRange']['start']
        ##TODO align timezone adjustment to state
        dt = (datetime.datetime.strptime(t, '%Y-%m-%dT%H:%M:%SZ')
              +datetime.timedelta(hours = 11)).strftime('%Y-%m-%d')

        for trip in d['trips']:
            try:
                td = {'id':trip['id'],
                      'date':dt,
                      'truck':trip['customDetails']['vanId'],
                      'status': trip['status'],
                      'session':trip['flags'][0],
                      'weight_cap':trip['capacity'].get('weight'),
                      'chilled_tote_cap':trip['capacity'].get('chilled-totes'),
                      'freezer_tote_cap':trip['capacity'].get('freezer-totes')}

            except:
                print(e)

            try:
                drops = trip['waypoints']
                for drop in drops:
                    if drop['type']=="VISIT":
                        det = drop
                        ords.append({'id':trip['id'],
                                     'session':trip['flags'][0],
                                    'orderNumber':det['id'],
                                   'ct':det['dropoff']['chilled-totes'],
                                    'ft':det['dropoff']['freezer-totes'],
                                    'w':det['dropoff']['weight']
                                   })
                    
                    ##Need to check for a reload allowed flag, not if it already has a reload trip
                    if drop['type']=="RELOAD":
                        td['reload'] = True

            except Exception as e:
                print(e)

            trs.append(td)


    to = pd.DataFrame(ords)
    tr = pd.DataFrame(trs)
    return to, tr


def get_schedule_full(data):
    # Extracting waypoints from each route
    routes = []
    for route in data[0]['trips']:
        for waypoint in route['waypoints']:
            route_data = {
                'route_id': route['id'],
                'waypoint_id': waypoint['id'],
                'waypoint_type': waypoint['type'],
                'latitude': waypoint['location']['latitude'],
                'longitude': waypoint['location']['longitude'],
                'service_time': waypoint['serviceTime'],
                'pickup_chilled_totes': waypoint['pickup']['chilled-totes'],
                'pickup_freezer_totes': waypoint['pickup']['freezer-totes'],
                'pickup_weight': waypoint['pickup']['weight'],
                'dropoff_chilled_totes': waypoint['dropoff']['chilled-totes'],
                'dropoff_freezer_totes': waypoint['dropoff']['freezer-totes'],
                'dropoff_weight': waypoint['dropoff']['weight'],
                'distance_to_next': waypoint['travel']['toNext']['distance'],
                'duration_to_next': waypoint['travel']['toNext']['duration'],
                'order_source': waypoint['customDetails']['orderSource'] if 'customDetails' in waypoint else None,
                'start_time': waypoint['slot']['available'][0]['start'] if waypoint['slot']['available'] else None,
                'end_time': waypoint['slot']['available'][0]['end'] if waypoint['slot']['available'] else None,
                'post_code': waypoint['customDetails']['postCode'] if 'customDetails' in waypoint else None,
                'suburb': waypoint['customDetails']['suburb'] if 'customDetails' in waypoint else None
            }
            routes.append(route_data)

    # Creating DataFrame
    df = pd.DataFrame(routes)

    # Displaying the DataFrame
    return df













# mon = [2687, 4418, 2654, 1556, 5618, 2623, 2510, 2566]
# sb = SataliaBase(SATALIA_KEY, SATALIA_URL)
# ##TO DO - clarify time period necessary for different timezones/sessions
# params = {
#         "start": "2024-02-18T17:00:00Z",
#          "end":"2024-02-20T12:30:00Z"
#         }
# data = sb.get_schedule(mon[7], params)
# # data

# to, tr = parse_schedule(data)
# #two tables - truck details (weight cap, reload, # drops)
##TO DO - find reload flag



# tr.merge(to.groupby(['id', 'session'])[['ct', 'ft', 'w']].sum().reset_index(),
#         on = 'id', how = 'left').\
#     assign(wp = lambda r: r.w/r.weight_cap).\
#     assign(ctp = lambda r: r.ct/r.chilled_tote_cap)

# tr.fillna(0).merge(to.groupby(['id', 'session'])[['ct', 'ft', 'w']].sum().reset_index(),
#         on = 'id', how = 'left').\
#         fillna(0).\
#         groupby(['date', 'session_x', 'reload'])[['weight_cap', 'chilled_tote_cap', 'ct', 'w']].sum().\
#         assign(wp = lambda r: r.w/r.weight_cap).\
#         assign(ctp = lambda r: r.ct/r.chilled_tote_cap)








# to
# tr
# i = 0
# str(data).find("1143", i+1)
# i=15899
# str(data)[i:i+1000]

# data[0].pop('trips')
# data[0]


# to.query('orderNumber=="184812055"')
# to.query('session=="AM"').sort_values('w', ascending = False)

# tr.groupby('truck').id.count()
# tr.query('truck=="8881-001-AM"')

# res = []
# for i, d in enumerate(data):
#     for j, trip in enumerate(d['trips']):
#         if trip['customDetails']['vanId']=="8881-006-AM":
#             print(i,j)

# #         drops = trip['waypoints']
# #         for drop in drops:
# #             if drop['type']=="RELOAD":
# #                 print(drop)

# data[0]['trips'][2]
# data[1]['trips'][2]

# data[0].pop('trips')
# data[0]['trips'][0]['reload']

# str(data[0])[:500]

# # data - bays loadingSchedule objective

# slot': {'selected': 0,
#     'available': [{'start': '2023-12-19T23:55:00Z',
#       'end': '2023-12-20T02:42:00Z'}]},
# data[0]['trips'][2]['waypoints']=None
# data[0]['trips'][2]['waypoints']=None


# data[0]['trips'][0]['slot']['available'][data[0]['trips'][0]['slot']['selected']]['start'] ##timezone??
# data[0]['metadata']['timeRange']['end'] #end



