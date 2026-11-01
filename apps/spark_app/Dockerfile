# Bitnami's Spark 3.5 image (Spark + Java on a minimal Debian base)
FROM bitnami/spark:3.5

USER root
WORKDIR /opt/bitnami/spark/app

# Ivy cache: Spark stores JARs resolved from --packages here (mounted as a volume in Compose).
RUN mkdir -p /opt/bitnami/spark/app/ivy

# The streaming job plus its pure transformation functions (imported as `from transforms import ...`).
COPY flight_stream.py transforms.py ./
RUN chmod 644 flight_stream.py transforms.py
