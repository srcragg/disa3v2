
# %%
import pandas as pd
import sqlite3
import datetime
import time
import paho.mqtt.client as mqtt
import json
from dataclasses import dataclass, field
import os
import yaml
from yaml import CLoader as Loader
from typing import Dict

# %%
config_path  = "config.yaml"
old_config_mtime = 0  # mtime is time file is modified

@dataclass
class config_default:
    port: int = 0
    stop_running: bool = False
    cell_name: str  = ''
    shift_times  = {'0':[(4,30),(17,0)],
                    '1':[(4,30),(17,0)],
                    '2':[(4,30),(17,0)],
                    '3':[(4,30),(17,0)],
                    '4':[(4,30),(16,30)]}
    shift_times_converted = {}
    db_name: str = 'disa3.db'


    

    def convert_shift_time(self):
        now = datetime.datetime.now()
        for k, v  in self.shift_times.items():
            self.shift_times_converted[k] = [datetime.datetime(year = now.year,
                                                                 month = now.month,
                                                                 day = now.day,
                                                                 hour = v[0][0],
                                                                 minute = v[0][1]),
                                                datetime.datetime(year = now.year,
                                                                 month = now.month,
                                                                 day = now.day,
                                                                 hour = v[1][0],
                                                                 minute = v[1][1])]


def get_config(config_path, old_config_mtime, config):
    '''checks if config file has been updated and updates config instance of config_update class'''
    try:
        mtime = os.path.getmtime(config_path)
    except:
        return old_config_mtime, config
    if mtime> old_config_mtime:
        try:
            configs =[]
            with open(config_path, 'r') as files:
                files = yaml.safe_load_all(files)
                for file in files:
                    configs.append([file])
            yaml_data = configs[1][0]
            config.stop_running = yaml_data['stop_running']
            config.cell_name = yaml_data['cell_name']
            config.shift_times = yaml_data['shift_times']
            config.db_name = yaml_data['db_name']
            old_config_mtime = mtime
        except:
            return old_config_mtime, config
    return old_config_mtime, config





def get_data_from_db(con, start_time, end_time = None):
    try:
        cur = con.cursor()
    except:
        con = sqlite3.connect(f"{config.db_name}")
    if end_time == None:
        data = pd.read_sql(f'select * from counter where id > {start_time}', con)
    else:
        data = pd.read_sql(f'select * from counter where id > {start_time} and id < {end_time}', con)
    data['timestamp'] = pd.to_datetime(data['timestamp'], unit = 's')
    data['running'] = ( data['timestamp']-data['timestamp'].shift(1)).dt.total_seconds()<45

    return data
 

def process_data(data):
    try:
        image = f'../images/{data['id'].max()}.jpg'
    except:
        pass
    message = {}

    message['total_stopped_time'] = data[data['running']==False]['ct'].sum()
    message['total_cycling_time'] = data[data['running']==True]['ct'].sum()
    message['total_1_parts_cast'] = data['part_1'].sum()+0.01 
    message['total_2_parts_cast'] = data['part_2'].sum()+0.01 
    message['total_cycles'] = len(data)
    message['time_on_hold'] = (datetime.datetime.now() - data['timestamp'].max()).seconds -3600
    message['ave_cycle_last_10'] = data['ct'][-10:].mean()

    return message






config = config_default()
old_config_mtime, config = get_config(config_path, old_config_mtime, config)
config.convert_shift_time()
# %%
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f'{config.cell_name}_processor_to_mqtt_v1', clean_session = False)
client.username_pw_set(username = 'broker1', password='tdfcyclecounter')
client.will_set(f'tdg/tdf/{config.cell_name}/cycle_counterv2/status', 'offline', retain=True, qos = 2)
client.reconnect_delay_set(min_delay=1, max_delay=120)
client.connect('10.0.1.207', 1883, 60)
client.loop_start()

client.publish(f'tdg/tdf/{config.cell_name}/cycle_counterv2/status', 'online', retain=True, qos = 2)


running_toggle = 0
old_total_cycles_shift = 0
old_total_cycles = 0

con = sqlite3.connect(f"{config.db_name}")


# %%

while True:
    #c+=1
    old_config_mtime, config = get_config(config_path, old_config_mtime, config)
    config.convert_shift_time()
    current_time = datetime.datetime.now()
    target_date = datetime.datetime(year = current_time.year,
                                      month = current_time.month,
                                      day = current_time.day,
                                      hour = 4,
                                      minute = 0).timestamp()

    shift_start_time = config.shift_times_converted[str(current_time.weekday())][0].timestamp()
    shift_end_time = config.shift_times_converted[str(current_time.weekday())][1].timestamp()


# running data since 04:00
    data = get_data_from_db(con, target_date)
    total_cycles = len(data)
    if len(data) <10: # check if data is present and sufficient
        time.sleep(10)
        continue

    processed_data = process_data(data)
    if total_cycles != old_total_cycles:
        message_to_send = {k:v for k, v in processed_data.items()}
        client.publish(f"tdg/tdf/{config.cell_name}/cycle_counterv2/daily_cycle_data", json.dumps(message_to_send), retain=True, qos=2)
        old_total_cycles = total_cycles

 
    if processed_data['time_on_hold']>=45:
        #status_holder.markdown('# :red[NOT RUNNING]')
        if running_toggle == 1:
            running_toggle = 0
            message = json.dumps({'timestamp':current_time.timestamp(),'running':'false'})
            client.publish(f"tdg/tdf/{config.cell_name}/cycle_counterv2/running", message, retain=True, qos=2)

    else:
        #status_holder.markdown('# :green[RUNNING]')
        if running_toggle == 0: 
            running_toggle = 1
            message = json.dumps({'timestamp':current_time.timestamp(),'running':'true'})
            client.publish(f"tdg/tdf/{config.cell_name}/cycle_counterv2/running", message, retain=True, qos=2)



# running data within normal shift times
    
    data_shift = get_data_from_db(con, shift_start_time, shift_end_time)
    total_cycles_shift = len(data_shift)
    if len(data_shift) <10: # check if data is present and sufficient
        time.sleep(10)
        continue

    processed_shift_data = process_data(data_shift)

    if total_cycles_shift != old_total_cycles_shift:
        message_to_send = {k:v for k, v in processed_shift_data.items()}
        message_to_send["shift_start_human"] = config.shift_times_converted[str(datetime.datetime.now().weekday())][0].strftime('%d-%m-%y %H:%M:%S'),
        message_to_send["shift_end_human"] = config.shift_times_converted[str(datetime.datetime.now().weekday())][1].strftime('%d-%m-%y %H:%M:%S'),
        message_to_send["shift_start"] = config.shift_times_converted[str(datetime.datetime.now().weekday())][0].timestamp(),
        message_to_send["shift_end"] = config.shift_times_converted[str(datetime.datetime.now().weekday())][1].timestamp()
        client.publish(f"tdg/tdf/{config.cell_name}/cycle_counterv2/shift_cycle_data", json.dumps(message_to_send), retain=True, qos=2)
        old_total_cycles_shift = total_cycles_shift
 

    time.sleep(1)

    if config.stop_running == True:
        break


# %%
