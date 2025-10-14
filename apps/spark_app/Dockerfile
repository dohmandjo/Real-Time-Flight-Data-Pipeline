FROM bitnami/spark:3.5
USER root
# USER 1001
WORKDIR /opt/bitnami/spark/app
RUN mkdir -p /opt/bitnami/spark/app/ivy
COPY flight_stream.py .
RUN chmod 644 /opt/bitnami/spark/app/flight_stream.py


# # remove any leftovers named spark-app / flight_spark
# docker rm -f spark-app flight_spark 2>/dev/null || true

# # (optional) prune orphan networks from earlier attempts
# docker network prune -f