FROM bitnami/spark:latest
WORKDIR /app
COPY preprocessing/main.py /app/main.py
COPY preprocessing/jars/postgresql-42.7.7.jar /opt/spark/jars/postgresql-42.7.7.jar
CMD ["python", "/app/main.py"]
