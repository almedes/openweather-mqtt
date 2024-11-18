FROM --platform=linux/arm/v7 rsaikali/openweather-mqtt:latest
RUN pip install python-dotenv requests==2.28.1 urllib3==1.26.15

# Copy the modified script to the container, replacing the original one
COPY custom_openweather_mqtt.py /opt/openweather_mqtt.py
