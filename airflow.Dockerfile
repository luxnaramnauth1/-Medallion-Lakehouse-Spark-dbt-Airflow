FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java
ENV JAVA_HOME=/opt/java

USER airflow
ENV PYTHONDONTWRITEBYTECODE=1 DBT_SEND_ANONYMOUS_USAGE_STATS=false
# Spark jobs run in local mode inside the Airflow container
RUN pip install --no-cache-dir pyspark==3.5.1 delta-spark==3.1.0
# dbt lives in its own venv to avoid dependency clashes with Airflow
RUN python -m venv /home/airflow/dbt_venv \
 && /home/airflow/dbt_venv/bin/pip install --no-cache-dir "dbt-core~=1.8.0" "dbt-spark[PyHive]~=1.8.0"
