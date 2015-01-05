# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <markdowncell>

# # CBIBS
# ## Plot the results of the CBIBC QARTOD QC Tests
# 
# ### Procedure
# * Connect to CBIBS database
# * Get raw wave height data from a station
# * Plot raw data and QC'ed data for a series of QC tests
# 
# ### Primary flags for QARTOD
# 
#     GOOD_DATA = 1
#     UNKNOWN = 2
#     SUSPECT = 3
#     BAD_DATA = 4
#     MISSING = 9

# <codecell>

import psycopg2
import numpy as np
from urllib import quote
import qc
import quantities as q
import pandas as pd
import StringIO
from IPython import embed
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import folium
from IPython.display import HTML

# <markdowncell>

# ### Connect to database

# <codecell>

con_str = 'HIDDEN' #Enter connection string here

conn = psycopg2.connect(con_str)
cur = conn.cursor()
cur.execute("""SELECT id, site_name FROM cbibs.d_station WHERE site_code != 'unknown'""")
station_ids = cur.fetchall()
cur.execute("select id, actual_name from cbibs.d_variable where actual_name IN ('sea_surface_wave_significant_height')")
var_ids = cur.fetchall()

obs_df = []

# for station_id in station_ids:
# Just look at the first station
station_id = station_ids[1]
print(station_id[:])

var = var_ids[0]
print(var[1])
cur.execute("""select * from (select o.id, measure_ts,
            obs_value, l.longitude lon, l.latitude lat
            from cbibs.f_observation o JOIN cbibs.d_location l
            ON (o.d_location_id = l.id) WHERE d_station_id = %s
            AND d_variable_id = %s
            ORDER BY measure_ts DESC LIMIT 50000) t ORDER BY measure_ts;""",
            (station_id[0], var[0]))

arr = np.fromiter(cur, dtype=[('id', 'i4'), ('timestamp', 'datetime64[us]'),
                            ('obs_val', 'f8'),('lon', 'f8'), ('lat', 'f8')]) # , count=500)
print("Length is %s" % len(arr))
obs_df = pd.DataFrame(arr, index=arr['timestamp'])


conn.close()

# <markdowncell>

# ## Location Test
# Checks that longitude and latitude are within reasonable bounds defaulting to lon = [-180, 180] and lat = [-90, 90]. Optionally, check for a maximum range parameter in great circle distance defaulting to meters which can also use a unit from the quantities library

# <codecell>

bbox=[[-76.5, 39], [-76, 39.5]]
loc_flags = qc.location_set_check(arr['lon'], arr['lat'], bbox_arr=bbox, range_max=(1.00 *q.kilometer))

# <markdowncell>

# #### Define some leaflet map functions

# <codecell>

def get_coordinates(bounding_box):
    """Create bounding box coordinates for the map."""
    coordinates = []
    coordinates.append([bounding_box[1], bounding_box[0]])
    coordinates.append([bounding_box[1], bounding_box[2]])
    coordinates.append([bounding_box[3], bounding_box[2]])
    coordinates.append([bounding_box[3], bounding_box[0]])
    coordinates.append([bounding_box[1], bounding_box[0]])
    return coordinates

def inline_map(m):
    """From http://nbviewer.ipython.org/gist/rsignell-usgs/
    bea6c0fe00a7d6e3249c."""
    m._build_map()
    srcdoc = m.HTML.replace('"', '&quot;')
    embed = HTML('<iframe srcdoc="{srcdoc}" '
                 'style="width: 100%; height: 500px; '
                 'border: none"></iframe>'.format(srcdoc=srcdoc))
    return embed

# <markdowncell>

# ### Draw a map and plot the bounding box and station location (first 10 values...)

# <codecell>

m = folium.Map(location=[arr["lat"][0], arr["lon"][0]], zoom_start=8)
m.line(get_coordinates(np.asarray(bbox).reshape(-1)), line_color='#FF0000', line_weight=5)
popup_string = ('<b>Station:</b><br>'+ str(station_id[1]))
for lat, lon in zip(arr["lat"][0:9], arr["lon"][0:9]):
    m.simple_marker([lat, lon], marker_color='purple', popup=popup_string)
bad_vals = np.where(loc_flags>1)
print bad_vals
inline_map(m)

# <markdowncell>

# ### Plot the flag values to see if any are suspect...

# <codecell>

# Create a pandas dataframe to store and plot the data
data = {}
data['time'] = arr['timestamp']
data['flags'] = loc_flags

data_df = pd.DataFrame(data, index=data['time'])
fig, ax = plt.subplots(figsize=(12, 6))
data_df['flags'].plot(ax=ax, color='k', title='Location QC Flag', ylim=[0,5])

# <markdowncell>

# #### All the location data looks good!

# <markdowncell>

# ## Gross Range test
# Given sensor minimum/maximum values, flag data that falls outside of range as bad data.  Optionally also flag data which falls outside of a user defined range

# <codecell>

# Specify the gross range (0-0.8m for this demonstration)
gross_range = (0, 0.8)
range_flags = qc.range_check(arr['obs_val'], gross_range)

# <markdowncell>

# ### Plot the flag values to see if any are suspect...
# #### Then plot the raw and qc'ed time series data

# <codecell>

# Create a pandas dataframe to store and plot the data
raw_data = {}
raw_data['time'] = arr['timestamp']
raw_data['flags'] = range_flags
raw_data['raw'] = arr['obs_val']

raw_df = pd.DataFrame(raw_data, index=raw_data['time'])
fig, ax = plt.subplots(3,1,figsize=(12, 21))
raw_df['flags'].plot(ax=ax[0], color='k', title='Gross Range QC Flag', ylim=[0,5])
raw_df['raw'].plot(ax=ax[1], color='r', title='sea_surface_wave_significant_height - RAW')

good_data = np.where(range_flags == 1)
qc_data = {}
qc_data['time'] = arr['timestamp'][good_data]
qc_data['qc'] = arr['obs_val'][good_data]
qc_df = pd.DataFrame(qc_data, index=qc_data['time'])
qc_df['qc'].plot(ax=ax[2], color='b', title='sea_surface_wave_significant_height - QC', ylim=[0,1])

# <markdowncell>

# #### Note that the QC data does not contain values over 0.8

# <markdowncell>

# ## Spike test
# Determine if there is a spike at data point n-1 by subtracting the midpoint of n and n-2 and taking the absolute value of this quantity, seeing if it exceeds a threshold for suspect and bad data. 
# Values which do not exceed either threshold are flagged good, values which exceed the low threshold are flagged suspect, and values which exceed the high threshold are flagged bad. The flag is set at point n-1.

# <codecell>

warning_threshold = 0.15
bad_threshold = 0.5
spike_flags = qc.spike_check(arr['obs_val'], warning_threshold, bad_threshold)

# <markdowncell>

# ### Plot the flag values to see if any are suspect...
# #### Then plot the raw and qc'ed time series data, zooming in on the first 'suspect data'

# <codecell>

# Create a pandas dataframe to store and plot the data
raw_data = {}
raw_data['time'] = arr['timestamp']
raw_data['flags'] = spike_flags
raw_data['raw'] = arr['obs_val']

raw_df = pd.DataFrame(raw_data, index=raw_data['time'])
fig, ax = plt.subplots(3,1,figsize=(12, 21))
raw_df['flags'].plot(ax=ax[0], color='k', title='Spike Test QC Flag', ylim=[0,5])


good_data = np.where(spike_flags == 1)
bad_data = np.where(spike_flags == 3)

start = bad_data[0][0]-25
stop = bad_data[0][0]+25

# Zoom in on the first 'suspect data'
raw_df['raw'][start:stop].plot(ax=ax[1], color='r', marker='o', title='sea_surface_wave_significant_height - RAW')
qc_data = {}
qc_data['time'] = arr['timestamp'][good_data]
qc_data['qc'] = arr['obs_val'][good_data]
qc_df = pd.DataFrame(qc_data, index=qc_data['time'])
qc_df['qc'][start:stop-1].plot(ax=ax[2], color='b', marker='o', title='sea_surface_wave_significant_height - QC', ylim=[0, 0.4])

# <markdowncell>

# #### From the plots, you can see some of the data exceeded the 'suspect' data thresold, hence the qc flag values of 3
# 
# #### You can see the spike absent from the zoomed in QC plot when the suspect data is removed.

# <markdowncell>

# ## Flat Line test
# Check for repeated consecutively repeated values within a tolerance eps

# <codecell>

rep_flags = qc.flat_line_check(arr['obs_val'], 3, 5, 0.001)

# <markdowncell>

# ### Plot the flag values to see if any are suspect...
# #### Then plot the raw and qc'ed time series data, zooming in on the first 'suspect data'

# <codecell>

# Create a pandas dataframe to store and plot the data
raw_data = {}
raw_data['time'] = arr['timestamp']
raw_data['flags'] = rep_flags
raw_data['raw'] = arr['obs_val']

raw_df = pd.DataFrame(raw_data, index=raw_data['time'])
fig, ax = plt.subplots(3,1,figsize=(12, 21))
raw_df['flags'].plot(ax=ax[0], color='k', title='Flat Line Test QC Flag', ylim=[0,5])

good_data = np.where(rep_flags == 1)
bad_data = np.where(rep_flags == 4)

start = bad_data[0][0]-25
stop = bad_data[0][0]+25

# Only plot the data near the suspect data to show the flat line
raw_df['raw'][start:stop].plot(ax=ax[1], color='r', marker='o', title='sea_surface_wave_significant_height - RAW')
qc_data = {}
qc_data['time'] = arr['timestamp'][good_data]
qc_data['qc'] = arr['obs_val'][good_data]
qc_df = pd.DataFrame(qc_data, index=qc_data['time'])
qc_df['qc'][start-2:stop-5].plot(ax=ax[2], color='b', marker='o', title='sea_surface_wave_significant_height - QC')

# <markdowncell>

# #### The Flat line QC test removed 2 data points from the flat line data around 12:00 on Sep 29

# <codecell>


