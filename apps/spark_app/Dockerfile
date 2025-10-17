# Use Bitnami’s Spark 3.5 image (includes Spark, Java, and a minimal Debian base)
FROM bitnami/spark:3.5

# Switch to root so we can create dirs / copy files with proper permissions
USER root

# Work in Spark’s app directory (where we’ll place our streaming script)
WORKDIR /opt/bitnami/spark/app

# Pre-create an Ivy cache dir; Spark uses this to store resolved JARs from --packages
RUN mkdir -p /opt/bitnami/spark/app/ivy

# Copy the PySpark streaming job into the image
COPY flight_stream.py .

# Make the script world-readable (container runs as non-root in some contexts)
RUN chmod 644 /opt/bitnami/spark/app/flight_stream.py