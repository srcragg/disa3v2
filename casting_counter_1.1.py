import manageRTSP as rtsp
import cv2
from collections import deque
import numpy as np
import time
from datetime import datetime
from statistics import mean
import sqlite3
import paho.mqtt.client as mqtt
import json
import yaml
from yaml import CLoader as Loader
from dataclasses import dataclass, field
import os
import logging

# This script is designed to run on a remote server with a networked IP camera supporting RTSP. 
# It is designed to count the number of parts that pass a certain point on a conveyor belt. 
# The script uses optical flow to measure the speed of the conveyor belt and a colour segmentation algorithm to detect the presence of parts. 
# The script uses a configuration file to set the parameters for the optical flow and colour segmentation algorithms. 
# The script also uses a SQLite database to store the count data and a MQTT broker to send the count data to a remote server. 
# The script is designed to run continuously and to be restarted automatically if it crashes. 
# The script also has a setup mode that allows the user to adjust the parameters of the optical flow and colour segmentation algorithms in real time.

def get_config(config_path, old_config_mtime, config):
    '''Checks if config file has been updated and updates config instance of config_default class'''
    try:
        mtime = os.path.getmtime(config_path)
    except Exception as e:
        logging.error(f"Error getting modification time of config file: {e}")
        return old_config_mtime, config

    if mtime > old_config_mtime:
        try:
            with open(config_path, 'r') as file:
                configs = list(yaml.safe_load_all(file))
                if configs:
                    config = config_default(**configs[0])
            old_config_mtime = mtime
        except Exception as e:
            logging.error(f"Error loading config file: {e}")

    return old_config_mtime, config


@dataclass
class config_default:
    setup_mode: bool =  False
    video: bool = True
    stop_running: bool  = False
    HSLlower_1: list[int] =  field(default_factory= lambda: [136, 62, 114])
    HSLupper_1: list[int] =  field(default_factory= lambda: [79, 189, 243])
    ul_bound_hsv: list[int] =  field(default_factory= lambda: [222,204])
    lr_bound_hsv: list[int] =  field(default_factory= lambda: [240,270])
    HSLlower: list[int] =  field(default_factory= lambda:  [13, 23, 244])
    HSLupper: list[int] =  field(default_factory= lambda:  [35, 255, 255])
    ul_bound_hsv_2: list[int] =  field(default_factory= lambda: [222,204])
    lr_bound_hsv_2: list[int] =  field(default_factory= lambda: [240,270])
    db_name: str =  "disa3.db"
    db_table: str =  "counts"
    camera_url: str =  None
    ul_bound_converyor: list[int] =  field(default_factory= lambda:  [600, 340])
    lr_bound_conveyor: list[int] =  field(default_factory= lambda:  [630, 360])
    ul_bound_flow: list[int] =  field(default_factory= lambda:  [90, 180])
    lr_bound_flow: list[int] =  field(default_factory= lambda: [540, 240])
    brightness_thresh: int  = 245
    cell_name: str = "disa3"
    device_name: str = "optical_counter"
    mqtt_username: str = ""
    mqtt_password: str = ""
    mqtt_host: str = ""
    mqtt_port: int = 1883
    image_dir_cleanup: bool  = False



def hsv_segmentation(frame, HSLlower, HSLupper):
    HSLlower = np.array(HSLlower, int)
    HSLupper = np.array(HSLupper, int)
    hsv = cv2.cvtColor(frame, cv2.COLOR_BGR2HSV)
    #create a mask for  colour using inRange function
    mask = cv2.inRange(hsv, HSLlower, HSLupper)

    #perform bitwise and on the original image arrays using the mask
    reshsv = cv2.bitwise_and(frame, frame, mask=mask)
    return reshsv

def brightness_thresh(frame, lower = 245):
    frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    thresh, frame = cv2.threshold(frame, lower, 255,	cv2.THRESH_BINARY)
    return frame

def optical_flow(prvs, next, mask, 
                 ul_bound_flow, 
                 lr_bound_flow, 
                 ul_bound_converyor,
                lr_bound_conveyor):

    flow = cv2.calcOpticalFlowFarneback(prvs, next, None, 0.5, 3, 15, 3, 5, 1.2, 0)
    mag, ang = cv2.cartToPolar(flow[..., 0], flow[..., 1])
    mask[..., 0] = ang*180/np.pi/2
    mask[..., 2] = cv2.normalize(mag, None, 0, 255, cv2.NORM_MINMAX)
    bgr = cv2.cvtColor(mask, cv2.COLOR_HSV2BGR)
    #flow_x = flow[:,:,0].mean()
    flow_y = flow[:,:,1].mean()
    flow_x = flow[:,:,0][ul_bound_flow[1]:lr_bound_flow[1], ul_bound_flow[0]:lr_bound_flow[0]].mean()
    #flow_x = flow[ul_bound_flow[1]:lr_bound_flow[1], ul_bound_flow[0]:lr_bound_flow[0]].mean()    
    flow_conv = flow[ul_bound_converyor[1]:lr_bound_conveyor[1], ul_bound_converyor[0]:lr_bound_conveyor[0]].mean()
    return bgr, mask, flow_x, flow_y, flow_conv

def image_directory_cleanup():
        try:
            logging.info('Trying image directory cleanup')
            os.system("image_clean_up.py")
            config.image_dir_cleanup = True
            logging.info('Image directory cleanup ran')
        except:
            logging.info('Image directory cleanup failed')
            pass

def initialise_mqtt():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f'{config.cell_name}_{config.device_name}_v1.1', clean_session = False)
    client.username_pw_set(username = config.mqtt_username, password= config.mqtt_password)
    client.will_set(f'tdg/tdf/{config.cell_name}/{config.device_name}/status', 'offline', retain=True, qos = 2)
    client.reconnect_delay_set(min_delay=1, max_delay=120)
    client.connect(config.mqtt_host, config.mqtt_port, 60)
    client.loop_start()
    return client

def on_disconnect(client, userdata, rc, properties=None):
    if rc != 0:
        logging.error("Unexpected disconnection.")
        client.loop_stop()
        time.sleep(5)

def main(old_config_mtime, config, prev_gray, frame_reader, mask, config_path = "config.yaml"):

    current_time = datetime.now()
    try:  # try to initialise mqtt client. If this fails, the script will continue to run without mqtt and retry to connect every 5 seconds in loop
        client = initialise_mqtt()
        client.on_disconnect = on_disconnect

        message = {'status':'online', 'timestamp':current_time.timestamp(), 'timestamp_human': current_time.strftime("%d-%m-%y %H:%M:%S")}
        client.publish(f'tdg/tdf/{config.cell_name}/{config.device_name}/status', json.dumps(message), retain=True, qos = 2)
    except:
        logging.error("MQTT client failed to initialise at startup")
        client = None
        pass


    data_old = 0 # holder to trigger save of performance data when in setup mode

    signal = deque([0]*10) # will be a 10 frame window
    sample_rate = deque([0]*100,100) # average sample rate
    ave_sample_rate = 10 # initial default value (driven, not driving)

    # record raw data, juat use for debugging - initialise here
    record_flow_x = []
    record_flow_conv = []
    record_hsv_1_mag = []
    record_hsv_2_mag = []
    ave_sample_rates = []
    perf_record = [] # list of dicts to store processed results

    # cutoff threshold for colour level
    hsv_thresh = 10

    conv_deque = deque([0]*5, 5) # conveyor velocity measured over 5 frames
    box_deque = deque([0]*30, 30) # box velocity measured over 20 frames (so that box made flag is carried into database write)
    time_toggle = 0
    hsv_sum = 0
    hsv_sum_1 = 0
    hsv_sum_2 = 0
    brightness_sum_1 = 0
    brightness_sum_2 = 0 
    last_part_1_time = datetime.now()  # use to avoid counting missing part 1 as a part 2


    t1_old = datetime.now()

    # check database exists and create if required
    con = sqlite3.connect(config.db_name)
    cur = con.cursor()
    cur.execute(f"CREATE TABLE IF NOT EXISTS {config.db_table}(id INTEGER PRIMARY KEY, timestamp, track_cycle, part_1, part_2, ct, sent)")
    con.close()

    font = cv2.FONT_HERSHEY_SIMPLEX
    ave_sample_rate = 10
     
    
    
    while(True):      

        
        if not client.is_connected():
            try:
                client.loop_stop()
                client.disconnect()
                logging.error("MQTT client disconnected")
            except Exception as e:
                logging.error(f"Error stopping or disconnecting MQTT client: {e}")
                pass
            try:
                client = initialise_mqtt()
                client.on_disconnect = on_disconnect
                message = {'status': 'online', 'timestamp': current_time.timestamp(), 'timestamp_human': current_time.strftime("%d-%m-%y %H:%M:%S")}
                client.publish(f'tdg/tdf/{config.cell_name}/{config.device_name}/status', json.dumps(message), retain=True, qos=2)
            except Exception as e:
                logging.error(f"MQTT client reconnect failed: {e}")
                pass

        cycle_start_time = time.time()
        old_config_mtime, config = get_config(config_path, old_config_mtime, config)

        frame = None
        while frame is None:
            try:
                for _ in range(2):
                    frame = frame_reader.get_frame()
                    if frame is None:
                        raise ValueError("Frame is None")
                    height, width, channels = frame.shape
                break
            except:
                logging.error("failed to capture frame from camera")
                frame_reader.stop_frame_reader()
                time.sleep(5)
                frame_reader.start_frame_reader()

 
        frame_org = frame.copy()
        frame_gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY) 


        # optical flow for conveyor signal

        rgb, mask, flow_x, flow_y, flow_conv = optical_flow(prev_gray, 
                                                frame_gray, 
                                                mask, 
                                                config.ul_bound_flow,
                                                config.lr_bound_flow,
                                                config.ul_bound_converyor,
                                                config.lr_bound_conveyor)
        prev_gray = frame_gray


        ## colour signal for part fill

        reshsv_1 = hsv_segmentation(frame_org, config.HSLlower_1, config.HSLupper_1)
        reshsv_2 = hsv_segmentation(frame_org, config.HSLlower, config.HSLupper)
        reshsv_1_grey = cv2.cvtColor(reshsv_1, cv2.COLOR_BGR2GRAY)
        reshsv_2_grey = cv2.cvtColor(reshsv_2, cv2.COLOR_BGR2GRAY)
        hsv_1_mean = reshsv_1_grey[config.ul_bound_hsv[1]:config.lr_bound_hsv[1], config.ul_bound_hsv[0]:config.lr_bound_hsv[0]].mean()
        hsv_2_mean = reshsv_2_grey[config.ul_bound_hsv_2[1]:config.lr_bound_hsv_2[1], config.ul_bound_hsv_2[0]:config.lr_bound_hsv_2[0]].mean()
        hsv_sum_1 +=hsv_1_mean
        hsv_sum_2 +=hsv_2_mean

        if hsv_sum_1 >= 300: hsv_sum_1 = 300
        if hsv_sum_2 >= 300: hsv_sum_2 = 300
    

        brightness = brightness_thresh(frame_org, config.brightness_thresh)
        brightness_1_mean = brightness[config.ul_bound_hsv[1]:config.lr_bound_hsv[1], config.ul_bound_hsv[0]:config.lr_bound_hsv[0]].mean()
        brightness_2_mean = brightness[config.ul_bound_hsv_2[1]:config.lr_bound_hsv_2[1], config.ul_bound_hsv_2[0]:config.lr_bound_hsv_2[0]].mean()
        brightness_sum_1 +=brightness_1_mean
        brightness_sum_2 +=brightness_2_mean

        if brightness_sum_1 >= 300: brightness_sum_1 = 300
        if brightness_sum_2 >= 300: brightness_sum_2 = 300

        ## convert to signals and write to database

        conv_deque.appendleft(flow_conv)
        box_deque.appendleft(flow_x)
        box_val = sum(box_deque)
        if box_val>10:
            made_box = 1
        else:
            made_box = 0
        

        if mean(conv_deque)>0.25 and time_toggle == 0:
            time_toggle = 1
            t1 = datetime.now()      
            hsv_sum_1 = 0
            hsv_sum_2 = 0
            brightness_sum_1 = 0
            brightness_sum_2 = 0
        if mean(conv_deque)<-0.25 and time_toggle == 1:
            time_toggle = 0
            t2 = datetime.now()
            cycle_length = t2-t1

            if brightness_sum_1>hsv_thresh:
                made_part_1 = True
                last_part_1_time = datetime.now()
            else:
                made_part_1 = False
            if hsv_sum_2>hsv_thresh and made_part_1 == False and ((datetime.now() - last_part_1_time).total_seconds() >=300): # avoids double counting man parts as auto
                made_part_2 = True
            else:
                made_part_2 = False

            if t1_old<t1:
                ct = t1-t1_old
                t1_old = t1
            id = int(t1.timestamp())
            cv2.imwrite(f'images/{id}.jpg', frame_org)
    
            data = (id, t1.timestamp(), cycle_length.total_seconds(), made_part_1, made_part_2, ct.total_seconds(), made_box)
            con = sqlite3.connect(f"{config.db_name}")
            cur = con.cursor()
            data_string = f"INSERT OR REPLACE INTO {config.db_table}(id, timestamp, track_cycle, part_1, part_2, ct, sent) VALUES (?,?,?,?,?,?,?)"
            cur.execute(data_string, data)
            con.commit()
            con.close()
            message = {'id' : id, 'timestamp' : t1.timestamp(), 'cycle_length' : cycle_length.total_seconds(), 
                    'part_1' : made_part_1, 'part2' : made_part_2, 'ct'  : ct.total_seconds(), 'box'  : made_box}
            
            if client.is_connected():
                client.publish(f"tdg/tdf/{config.cell_name}/{config.device_name}/cycle_data",json.dumps(message))
    
        ## Add variables to debug lists
        if config.setup_mode == True:
            record_flow_x.append(flow_x)
            record_flow_conv.append(flow_conv)
            record_hsv_1_mag.append(hsv_sum_1)
            record_hsv_2_mag.append(hsv_sum_2)

            if data!=data_old:
                perf_record.append({'time':t1, 'cycle_length':cycle_length, 'part_2':made_part_2,'part_1':made_part_1, 'ct':ct})

        ## display video

        if config.video == True:
                    
            cv2.putText(rgb, f'x_vel {mean(conv_deque):.2f}', (10,height-10), font, 1, (0, 255, 0), 2, cv2.LINE_AA)
            cv2.rectangle(rgb, config.ul_bound_flow, config.lr_bound_flow, (255,0,0), (2))
            cv2.rectangle(rgb, config.ul_bound_converyor, config.lr_bound_conveyor, (0,0,255), (2))
            cv2.imshow("dense optical flow", rgb) 

            cv2.rectangle(reshsv_1, config.ul_bound_hsv, config.lr_bound_hsv, (0,255,0), (2))
            cv2.rectangle(reshsv_2, config.ul_bound_hsv_2, config.lr_bound_hsv_2, (0,0,255), (2))
            hsv_image = cv2.addWeighted(reshsv_1,1,reshsv_2,1,0)
            cv2.imshow('hsv', hsv_image)
        
            cycle_time = time.time()-cycle_start_time
            sample_rate.append(cycle_time)
            ave_sample_rate = mean(sample_rate)
            cv2.putText(frame_org, f'frame rate {1/ave_sample_rate:.2f}', (10,height-10), font, 1, (0, 255, 0), 2, cv2.LINE_AA)
            cv2.imshow(f'frame_org', frame_org)

            brightness = cv2.cvtColor(brightness, cv2.COLOR_GRAY2BGR)
            cv2.rectangle(brightness, config.ul_bound_hsv, config.lr_bound_hsv, (0,255,0), (2))
            cv2.imshow('brightness', brightness)


        if datetime.fromtimestamp(cycle_start_time).hour == 1 and config.image_dir_cleanup == False:
            image_directory_cleanup()
        if datetime.fromtimestamp(cycle_start_time).hour == 2 and config.image_dir_cleanup == True:
            config.image_dir_cleanup == False


        
        if (cv2.waitKey(1) & 0xFF == ord('q')) or (config.video == False):
            cv2.destroyAllWindows()
            cv2.waitKey(1)
            time.sleep(0.1)
        
        if config.stop_running == True:
            break

if __name__ == '__main__':

    ## set up logging
    logging.basicConfig(filename='app_counter.log', 
                        level=logging.DEBUG, 
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        filemode='a')
    logging.debug('log started')


    ## get config file - will be updated on change in main loop
    config_path  = "config.yaml"
    old_config_mtime = 0

    # initialise config instance. Config gets updated within main loop if config file changes
    config = config_default()
    old_config_mtime, config = get_config(config_path, old_config_mtime, config)



    # initialise frame reader 
    frame_reader = rtsp.FrameReader(config.camera_url)
    frame_reader.start_frame_reader()
    first_frame = frame_reader.get_frame()

    prev_gray = cv2.cvtColor(first_frame, cv2.COLOR_BGR2GRAY)

    mask = np.zeros_like(first_frame)
    mask[..., 1] = 255

    main(old_config_mtime, config, prev_gray, frame_reader, mask)


    #cap.release()
    cv2.destroyAllWindows()