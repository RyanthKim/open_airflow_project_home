FROM apache/airflow:2.9.3

USER root
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONPATH=/opt/airflow:/opt/airflow/tasks:/opt/airflow/libs
RUN apt-get update && \
    apt-get install -y gcc g++ python3-dev openjdk-17-jdk tzdata curl && \
    apt-get clean

USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt