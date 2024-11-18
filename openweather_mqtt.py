# -*- coding: utf-8 -*-
import logging
import os
import time

import paho.mqtt.publish as publish
import requests

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Config from environment (see Dockerfile)
OPENWEATHER_APP_ID = os.getenv('OPENWEATHER_APP_ID', 'YOUR_OPENWEATHER_APP_ID')
OPENWEATHER_CITY_ID_1 = os.getenv('OPENWEATHER_CITY_ID_1', 'YOUR_OPENWEATHER_CITY_ID_1')
OPENWEATHER_CITY_ID_2 = os.getenv('OPENWEATHER_CITY_ID_2', 'YOUR_OPENWEATHER_CITY_ID_2')

MQTT_SERVICE_HOST_1 = os.getenv('MQTT_SERVICE_HOST_1', 'mosquitto.local')
MQTT_SERVICE_HOST_2 = os.getenv('MQTT_SERVICE_HOST_2', 'mosquitto.local')
MQTT_SERVICE_PORT_1 = int(os.getenv('MQTT_SERVICE_PORT_1', 1883))
MQTT_SERVICE_PORT_2 = int(os.getenv('MQTT_SERVICE_PORT_2', 1883))
MQTT_SERVICE_TOPIC_1 = os.getenv('MQTT_SERVICE_TOPIC_1', 'openweather')
MQTT_SERVICE_TOPIC_2 = os.getenv('MQTT_SERVICE_TOPIC_2', 'openweather')
MQTT_CLIENT_ID_1 = os.getenv('HOSTNAME_1', 'openweather-mqtt-service')
MQTT_CLIENT_ID_2 = os.getenv('HOSTNAME_2', 'openweather-mqtt-service')
MQTT_SERVICE_USER_1 = os.getenv('MQTT_SERVICE_USER_1')
MQTT_SERVICE_USER_2 = os.getenv('MQTT_SERVICE_USER_2')
MQTT_SERVICE_PW_1 = os.getenv('MQTT_SERVICE_PW_1')
MQTT_SERVICE_PW_2 = os.getenv('MQTT_SERVICE_PW_2')

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(name)s] %(levelname)8s %(message)s')
logger = logging.getLogger(MQTT_CLIENT_ID_1)
logger = logging.getLogger(MQTT_CLIENT_ID_2)

# Display config on startup
logger.debug("#" * 80)
logger.debug(f"# OPENWEATHER_APP_ID={OPENWEATHER_APP_ID}")
logger.debug(f"# OPENWEATHER_CITY_ID_1={OPENWEATHER_CITY_ID_1}")
logger.debug(f"# MQTT_SERVICE_HOST_1={MQTT_SERVICE_HOST_1}")
logger.debug(f"# MQTT_SERVICE_PORT_1={MQTT_SERVICE_PORT_1}")
logger.debug(f"# MQTT_SERVICE_TOPIC_1={MQTT_SERVICE_TOPIC_1}")
logger.debug(f"# MQTT_CLIENT_ID_1={MQTT_CLIENT_ID_1}")
logger.debug(f"# MQTT_SERVICE_USER_1={MQTT_SERVICE_USER_1}")
logger.debug(f"# MQTT_SERVICE_PW_1={MQTT_SERVICE_PW_1}")
logger.debug(f"# -------------------------------------------")
logger.debug(f"# OPENWEATHER_CITY_ID_2={OPENWEATHER_CITY_ID_2}")
logger.debug(f"# MQTT_SERVICE_HOST_2={MQTT_SERVICE_HOST_2}")
logger.debug(f"# MQTT_SERVICE_PORT_2={MQTT_SERVICE_PORT_2}")
logger.debug(f"# MQTT_SERVICE_TOPIC_2={MQTT_SERVICE_TOPIC_2}")
logger.debug(f"# MQTT_CLIENT_ID_2={MQTT_CLIENT_ID_2}")
logger.debug(f"# MQTT_SERVICE_USER_2={MQTT_SERVICE_USER_2}")
logger.debug(f"# MQTT_SERVICE_PW_2={MQTT_SERVICE_PW_2}")
logger.debug("#" * 80)


def flatten_dict(dictionary, delimiter='.'):
    dictionary_ = dictionary

    def unpack(parent_key, parent_value):
        if isinstance(parent_value, dict):
            return [(parent_key + delimiter + key, value) for key, value in parent_value.items()]
        elif isinstance(parent_value, list):
            d = []
            for i, v in enumerate(parent_value):
                for k, vv in v.items():
                    d.append((parent_key + delimiter + str(i) + delimiter + k, vv))
            return d
        else:
            return [(parent_key, parent_value)]

    while True:
        dictionary_ = dict(ii for i in [unpack(key, value) for key, value in dictionary_.items()] for ii in i)
        if all([not isinstance(value, dict) for value in dictionary_.values()]):
            break

    return dictionary_


if __name__ == "__main__":

    previous_last_update = 0

    while True:

        try:

            logger.info("Connecting to OpenWeather for fresh weather information.")
            url = f"http://api.openweathermap.org/data/2.5/weather?id={OPENWEATHER_CITY_ID_1}&appid={OPENWEATHER_APP_ID}&type=accurate&units=metric&lang=de"
            r = requests.get(url)
            data = r.json()

            # Hack: set default rain to 0 if no rain indicated
            data.setdefault('rain', {})
            data['rain'].setdefault('1h', 0)
            data['rain'].setdefault('3h', 0)

            if int(data['dt']) >= int(previous_last_update):
                previous_last_update = int(data['dt'])

                msgs1 = []
                for k, v in sorted(flatten_dict(data, delimiter='/').items()):
                    logger.info(f"{k:24} ---> {v}")
                    msgs1.append({'topic': f"{MQTT_SERVICE_TOPIC_1}/{k}", 'payload': str(v)})

                msgs2 = []
                for k, v in sorted(flatten_dict(data, delimiter='/').items()):
                    logger.info(f"{k:24} ---> {v}")
                    msgs2.append({'topic': f"{MQTT_SERVICE_TOPIC_2}/{k}", 'payload': str(v)})
            else:
                logger.info("No updated data from Openweather...")

            # Publish openweather results on given MQTT broker every second, so we can view it often,
            # but call Openweather API every ~1min (otherwise you'll get locked due to API rate limits)
            last_update = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(data['dt']))
            for i in range(60):
                logger.info(f"Publishing to {MQTT_SERVICE_HOST_1}:{MQTT_SERVICE_PORT_1} [last_update={last_update}]")
                logger.info(f"Publishing to {MQTT_SERVICE_HOST_2}:{MQTT_SERVICE_PORT_2} [last_update={last_update}]")
                # Building the parameters for the publish.multiple() call
                kwargs1 = {
                    'hostname': MQTT_SERVICE_HOST_1,
                    'port': MQTT_SERVICE_PORT_1,
                    'client_id': MQTT_CLIENT_ID_1
                }

                kwargs2 = {
                    'hostname': MQTT_SERVICE_HOST_2,
                    'port': MQTT_SERVICE_PORT_2,
                    'client_id': MQTT_CLIENT_ID_2
                }

                # Only add auth if user and password are available
                if MQTT_SERVICE_USER_1 and MQTT_SERVICE_PW_1:
                    kwargs1['auth'] = {'username': MQTT_SERVICE_USER_1, 'password': MQTT_SERVICE_PW_1}
                if MQTT_SERVICE_USER_2 and MQTT_SERVICE_PW_2:
                    kwargs1['auth'] = {'username': MQTT_SERVICE_USER_2, 'password': MQTT_SERVICE_PW_2}

                # Call publish.multiple with the appropriate parameters
                publish.multiple(msgs1, **kwargs1)
                publish.multiple(msgs2, **kwargs2)

                time.sleep(1)

        except Exception:
            logger.error("An error occured:", exc_info=True)
