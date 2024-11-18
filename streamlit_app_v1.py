import streamlit as st
import pandas as pd
import sqlite3
import datetime
import time
import matplotlib.pyplot as plt
import paho.mqtt.client as mqtt
import json
import pytz
import numpy as np



# client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,'disa3_counter', clean_session = False)
# client.username_pw_set(username = 'broker1', password='tdfcyclecounter')
# client.reconnect_delay_set(min_delay=1, max_delay=120)
# client.connect('127.0.0.1', 1883, 60)
# client.loop_start()

running_toggle = 0
old_total_cycles_shift = 0

st.title("DISA 3 prod Cycle Counting v2")

# rest of this needs to run in a data refresh loop

#with st.container():
col1, col2 = st.columns(2)
cur_time_holder = col1.empty()
time_last_cycled_holder = col1.empty()
time_on_hold_holder = col1.empty()
time_first_cycle_holder = col1.empty()
time_first_box_holder = col1.empty()
time_first_pour_holder = col1.empty()
status_holder = col1.empty()
ave_cycle_time_holder = col1.empty()
ave_cycle_time_holder_pph = col1.empty()
current_image = col2.empty()

col11, col12, col13, col14, col15, col16 = st.columns(6)
col11.markdown('**Total Stopped Time**')
total_stopped_time_holder = col11.empty()
col12.markdown('**Total Running Time**')
total_cycling_time_holder  = col12.empty()
col13.markdown('''**Unheated Boxes Today**     
               ''')
total_1_parts_cast_holder = col13.empty()
col14.markdown('**Heated Boxes Today**')
total_2_parts_cast_holder = col14.empty()

col15.markdown('**Total Machine Cycles**')
total_cycles_holder = col15.empty()
col16.markdown('**Average BPH Total Time**')
ave_overall_pph_holder = col16.empty()

st.subheader('Daily performance, 5 minute rolling average')
perf_chart = st.empty()

con = sqlite3.connect("disa3v1.db")
lon = pytz.timezone("Europe/London")

while True:

    current_time = datetime.datetime.now(lon)
    target_date = datetime.datetime(year = current_time.year,
                                      month = current_time.month,
                                      day = current_time.day,
                                      hour = 4,
                                      minute = 30).timestamp()
    try:
        cur = con.cursor()
    except:
        con = sqlite3.connect("disa3v1.db")
    data = pd.read_sql(f'select * from counter where id > {target_date}', con)
    data = data.iloc[1:,:]
    #con.close()
    if len(data)<5: # if no data in table
        time.sleep(10)
        continue

    data['timestamp'] = pd.to_datetime(data['timestamp'], unit = 's', utc = True)
    data['running'] = ( data['timestamp']-data['timestamp'].shift(1)).dt.total_seconds()<45
    data['ct2'] = data['timestamp'].diff(1).dt.total_seconds().fillna(0)
    data = data[data['ct2']>7]
    data = data[data['track_cycle']<7]
    total_cycles_shift = data['ct'].count()


    try:
        image = f'images/{data['id'].max()}.jpg'
    except:
        pass

    total_stopped_time = data[data['running']==False]['ct'].sum()
    total_cycling_time = data[data['running']==True]['ct'].sum()
    total_1_parts_cast = data['part_1'].sum()+0.01 
    total_2_parts_cast = data['part_2'].sum()+0.01 
    total_cycles = len(data)
    time_on_hold = (current_time - data['timestamp'].max()).seconds
    ave_cycle_last_10 = data['ct'][-10:].mean()

    time_first_cycle = data['timestamp'].min()
    time_fist_box = data[data['sent']==1].iloc[0,:]['timestamp'] 


    times_state = (data[data['part_1']==1].empty, data[data['part_2']==1].empty)
    match times_state:
        case (True, True):
            time_first_pour = None
        case (False, True):
            time_first_pour =  data[data['part_1']==1].iloc[0,:]['timestamp']
        case (True, False):
            time_first_pour =  data[data['part_2']==1].iloc[0,:]['timestamp']
        case (False, False):
            time_first_pour =  min(data[data['part_1']==1].iloc[0,:]['timestamp'], data[data['part_2']==1].iloc[0,:]['timestamp'])

    data['ave_pph'] = 3600/data['ct'].rolling(20).mean()

    data2 = data.copy()
    data2.set_index('timestamp', inplace = True)
    data2['rolling_ave'] = (3600/data2.rolling('10min', min_periods=3).mean())['ct']
    data2['rolling_ave_part_1'] = (3600/data2[data2['part_1']==1].rolling('10min', min_periods=3).mean())['ct']
    data2['rolling_ave_part_2'] = (3600/data2[data2['part_2']==1].rolling('10min', min_periods=3).mean())['ct']
    
    cur_time_holder.write(f'{current_time.strftime('%A %B %d %H:%M:%S')}')
    time_last_cycled_holder.write(f'Time last cycled {data['timestamp'].max().astimezone(lon).strftime('%H:%M:%S')}')
    time_on_hold_holder.write(f'Time since last cycle {time_on_hold} s')
    try:
        time_first_cycle_holder.write(f'First cycle {time_first_cycle.strftime('%H:%M:%S')}')
    except:
        time_first_cycle_holder.write(f'- Awaiting first cycle')
    try:
        time_first_box_holder.write(f'First box seen {time_fist_box.strftime('%H:%M:%S')}')
    except:
        time_first_box_holder.write(f'- Awaiting first box')
    try:
        time_first_pour_holder.write(f'First metal poured {time_first_pour.strftime('%H:%M:%S')}')
    except:
        time_first_pour_holder.write(f'- Awaiting first pour')

    if time_on_hold>=45:
        status_holder.markdown('# :orange[NOT RUNNING]')
        if time_on_hold>=300:
            status_holder.markdown('# :red[NOT RUNNING]')
            if running_toggle == 1:
                running_toggle = 0
                message = json.dumps({'timestamp':current_time.timestamp(),'running':'false'})
            # client.publish("tdg/tdf/disa3/cycle_counter/running", message, retain=True, qos=2)
        

    else:
        status_holder.markdown('# :green[RUNNING]')
        if running_toggle == 0: 
            running_toggle = 1
            message = json.dumps({'timestamp':current_time.timestamp(),'running':'true'})
            # client.publish("tdg/tdf/disa3/cycle_counter/running", message, retain=True, qos=2)

    
    ave_cycle_time_holder.write(f'Average cycle time last 10 cycles {ave_cycle_last_10:.2f} seconds')
    ave_cycle_time_holder_pph.markdown(f'## {3600/ave_cycle_last_10:.2f} bph')

    total_stopped_time_holder.write(f'{total_stopped_time//60:.0f} mins')
    total_cycling_time_holder.write(f'{total_cycling_time//60:.0f} mins')
    total_1_parts_cast_holder.write(f'{total_1_parts_cast:.0f}')
    total_2_parts_cast_holder.write(f'{total_2_parts_cast:.0f}')
    total_cycles_holder.write(f'{total_cycles}')
    ave_overall_pph_holder.write(f'{3600/((total_stopped_time + total_cycling_time)/total_cycles):.1f}')

    current_image.image(image)
    # perf_chart.line_chart(data, x = 'timestamp', y = 'ave_pph', 
    #                       y_label = 'Rolling BPH (20 cycles)',
    #                       x_label = 'Time (UTC)',
    #                       height = 300)
    perf_chart.line_chart(data2,  y = ['rolling_ave', 'rolling_ave_part_1','rolling_ave_part_2' ], 
                          color = [(1.0,0,0, 0.1), (0,1.0,0), (0,0,1.0)], 
                          y_label = 'Rolling BPH (5 min window)',
                          x_label = 'Time (UTC)',
                          height = 300)



    time.sleep(0.5)

