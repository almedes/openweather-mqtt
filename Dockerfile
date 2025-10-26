FROM python:3.11-slim

WORKDIR /opt

RUN pip install --no-cache-dir \
    python-dotenv \
    requests==2.28.1 \
    urllib3==1.26.15 \
    paho-mqtt

COPY custom_openweather_mqtt.py /opt/openweather_mqtt.py

CMD ["python", "/opt/openweather_mqtt.py"]

