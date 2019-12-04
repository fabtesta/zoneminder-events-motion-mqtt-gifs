#!/usr/bin/env python3
import datetime
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from shutil import copyfile
import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)s] [%(levelname)s] (%(threadName)-10s) %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


def parse_config(config_path):
    with open(config_path, 'r') as config_file:
        config_data = json.load(config_file)
    return config_data


def zm_get_video(ffmpeg_working_folder, zm_source_video_folder, event_video_prefix, event_id):
    source_video = '{}/{}/{}.mp4'.format(zm_source_video_folder, datetime.date.today(),
                                         event_video_prefix + '' + event_id)
    outfile_video = '{}/{}.mp4'.format(ffmpeg_working_folder, event_id)

    logging.info('Copying video for event id %s from %s to %s .....', source_video, outfile_video)
    copyfile(source_video, outfile_video)
    logging.info('.....done')
    return outfile_video


def convert_zm_video_to_gif(scale, skip_first_n_secs, max_length_secs, input_video, output_gif):
    logging.info(
        'Converting to gif scale %i skip_first_n_secs %i max_length_secs %i input_video %s output_gif %s .....',
        scale, skip_first_n_secs, max_length_secs, input_video, output_gif)

    retcode = subprocess.call([
        "ffmpeg", "-stats", "-i", input_video, "-vf",
        "fps=15,scale={}:-1:flags=lanczos".format(scale),
        "-ss", "00:00:" + "{}".format(skip_first_n_secs).zfill(2), "-t", "{}".format(max_length_secs), "-y",
        str(output_gif)
    ])
    os.remove(input_video)
    logging.info('.....done')
    return retcode


class ZoneMinderMotionEventHandler:
    def __init__(self, config):
        self.config = config
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.username_pw_set(username=self.config["mqtt_user"], password=self.config["mqtt_pwd"])

        self.sub_connected = False

        self.mqtt_client.on_disconnect = self.on_disconnect
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.on_subscribe = self.on_subscribe
        self.mqtt_client.on_publish = self.on_publish

    def connect(self):
        try:
            logging.info('connecting to mqtt_server %s mqtt_port %i',
                         self.config["mqtt_server"], self.config["mqtt_port"])
            self.mqtt_client.connect_async(self.config["mqtt_server"],
                                           self.config["mqtt_port"])
            self.mqtt_client.loop_start()
        except Exception as err:
            logging.error('connect exception', err)
            self.disconnect()

    def disconnect(self):
        try:
            logging.info('disconnecting from mqtt_server %s mqtt_port %i',
                         self.config["mqtt_server"], self.config["mqtt_port"])
            self.mqtt_client.disconnect()
        except Exception as err:
            logging.error('disconnect exception', err)

    def on_connect(self, client, userdate, flag, rc):
        try:
            logging.info('connected to mqtt_server %s  mqtt_port %i',
                         self.config["mqtt_server"], self.config["mqtt_port"])

            for camera in self.config["zoneminder_cameras"]:
                logging.info('subscribing topic %s for camera %s', camera["id"],
                             self.config["mqtt_base_events_topic"] + '/' + camera["id"])
                self.mqtt_client.subscribe(self.config["mqtt_base_events_topic"] + '/' + camera["id"])

            self.sub_connected = True
        except Exception as err:
            logging.error('on_connect exception', err)
            self.disconnect()

    def on_disconnect(self, client, userdata, rc):
        logging.info('disconnected %s', str(rc))
        if rc != 0:
            logging.info("unexpected disconnection.")

        self.sub_connected = False
        self.mqtt_client.loop_stop()

    def on_subscribe(self, client, userdata, mid, granted_qos):
        logging.info('subscribed topic %s', str(mid))
        self.sub_connected = True

    def on_publish(self, client, userdata, mid):
        logging.info('published message %s', str(mid))
        self.sub_connected = True

    def publish_gif(self, camera_topic, gif):
        try:
            retcode = self.mqtt_client.publish(self.config["mqtt_base_gifs_topic"] + '/' + camera_topic,
                                               gif)
            logging.info('mqtt_client.publish retcode %s', retcode)
            return True
        except Exception as err:
            logging.error('publish exception', err)
            self.disconnect()

    def on_message(self, client, userdata, message):
        try:
            event_id = str(message.payload.decode("utf-8"))
            print("message received  ", event_id,
                  "topic", message.topic, "retained ", message.retain)

            camera_id = str(message.topic).replace(self.config["mqtt_base_events_topic"] + '/', '')
            camera = next((x for x in self.config["zoneminder_cameras"] if x["id"] == camera_id), None)
            event_video_prefix = camera["event_video_prefix"]
            logging.info('Getting event video for camera %s from base directory %s with event prefix %s', camera_id,
                         self.config["zoneminder_events_video_folder"], event_video_prefix)

            outfile_video = zm_get_video(self.config["ffmpeg_working_folder"],
                                         self.config["zoneminder_events_video_folder"],
                                         event_video_prefix, event_id)
            logging.info('Found video for camera %s', outfile_video)

            outfile_gif = '{}/{}.gif'.format(self.config["ffmpeg_working_folder"], event_id)
            convert_retcode = convert_zm_video_to_gif(camera["scale"],
                                                      camera["skip_first_n_secs"],
                                                      camera["max_length_secs"],
                                                      outfile_video, outfile_gif)
            if convert_retcode == 0:
                public_retcode = self.publish_gif(camera["id"], '{}.gif'.format(event_id))
                if public_retcode:
                    logging.info('Done processing event_id %s for camera %s', event_id, camera["id"])
                else:
                    logging.error('Invalid return code from mqtt publish for event id %i camera topic %s', event_id,
                                  camera["id"])
            else:
                logging.error('Invalid return code from ffmpeg subprocess call for event id %i', event_id)
        except Exception as err:
            logging.error('on message exception', err)
            self.disconnect()


def main():
    _, config_filename = sys.argv
    logging.info('Starting')
    logging.info('Parsing %s', config_filename)
    config = parse_config(config_filename)

    logging.info(
        'ZoneMinderMotionEventHandler subscriber creation on broker mqtt_server %s for '
        'folder zoneminder_events_video_folder %s',
        config["zoneminder_events_video_folder"], config["mqtt_server"])
    zm_events_handler = ZoneMinderMotionEventHandler(config)

    try:
        while True:
            if not zm_events_handler.sub_connected:
                zm_events_handler.connect()
            time.sleep(10)

    except KeyboardInterrupt:
        logging.info('KeyboardInterrupt')

    logging.info('Ending')


if __name__ == "__main__":
    main()
