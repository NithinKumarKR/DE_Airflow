FROM apache/airflow:3.0.1

USER root

RUN apt-get update && \
    apt-get -y install git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
