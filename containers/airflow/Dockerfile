# use official image
FROM apache/airflow:2.8.1-python3.9

USER root

# install Java and compile tools
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    build-essential \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# ensure pip installed executable files can be found
ENV PATH="/home/airflow/.local/bin:${PATH}"

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt