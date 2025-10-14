FROM python:3.12-slim
WORKDIR /apps
COPY run_producer.py .
RUN pip install --no-cache-dir requests kafka-python python-dateutil
CMD ["python", "run_producer.py", "--interval-seconds", "60"]