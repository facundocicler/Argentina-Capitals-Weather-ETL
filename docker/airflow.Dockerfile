FROM apache/airflow:2.9.1-python3.11

USER airflow
COPY requirements-airflow.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

USER root
RUN groupadd docker && usermod -aG docker airflow
USER airflow