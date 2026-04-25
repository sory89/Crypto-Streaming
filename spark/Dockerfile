FROM apache/spark:3.5.0-python3

USER root

RUN pip install --no-cache-dir requests

WORKDIR /opt/spark-app

COPY spark_streaming.py .
COPY submit.sh .
RUN chmod +x submit.sh

ENV PYTHONUNBUFFERED=1

CMD ["/bin/bash", "submit.sh"]
