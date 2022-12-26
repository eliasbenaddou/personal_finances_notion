FROM apache/airflow:latest-python3.9
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
RUN pip uninstall --yes dataclasses
USER root
RUN sudo chmod a+rwx /opt/airflow
USER airflow
