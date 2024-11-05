FROM uchimera.azurecr.io/cccs/ap/spark-kubernetes:3.5.1

COPY main.py main.py
COPY config.py config.py
COPY custom_ingestors custom_ingestors
COPY utilities utilities
COPY requirements.txt requirements.txt

RUN python -m pip install -r requirements.txt