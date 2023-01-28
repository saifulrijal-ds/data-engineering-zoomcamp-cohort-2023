FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_data.py ingest_data.py
# COPY green_trip_ingest.py green_trip_ingest.py

# ENTRYPOINT ["bash"]
ENTRYPOINT ["python", "ingest_data.py"]
# ENTRYPOINT ["python", "green_trip_ingest.py"]