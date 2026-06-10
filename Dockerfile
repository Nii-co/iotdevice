FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY iot_device_simulator.py .
COPY simulator_config.json .

# 默认多设备模式, 通过环境变量覆盖参数
ENV MODE="multi"
ENV CONFIG_PATH="simulator_config.json"
ENV INTERVAL=""
ENV COUNT=""
ENV DRY_RUN=""

CMD python iot_device_simulator.py \
    --mode ${MODE} \
    --config ${CONFIG_PATH} \
    ${INTERVAL:+--interval $INTERVAL} \
    ${COUNT:+--count $COUNT} \
    ${DRY_RUN:+--dry-run}
