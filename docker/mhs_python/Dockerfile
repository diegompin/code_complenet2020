FROM puckel/docker-airflow:1.10.4
MAINTAINER Diego Pinheiro diegompin@gmail.com
#USER root


#COPY ./docker/mhs_python/config/DEL_airflow.cfg DEL_airflow.cfg
COPY ./docker/mhs_python/requirements.txt requirements.txt
#COPY requirements.txt requirements.txt

COPY ./docker/mhs_python/config/airflow.cfg /usr/local/airflow/airflow.cfg
#COPY config/airflow.cfg airflow.cfg
#ENV PYTHONPATH "${PYTHONPATH}:${AIRFLOW_HOME}"
USER root
RUN apt-get -y update && apt-get -y install build-essential libxml2-dev zlib1g-dev python-dev python-pip pkg-config libffi-dev libcairo-dev

RUN mkdir -p /usr/share/man/man1 && \
    apt-get update -y && \
    apt-get install -y openjdk-8-jdk

USER airflow

RUN ["pip", "install", "--user", "-r",  "requirements.txt"]

#RUN apt install build-essential python-dev libxml2 libxml2-dev zlib1g-dev


WORKDIR .
COPY ./ ./
ENV PYTHONPATH "${PYTONPATH}:./"


#USER airflow
#ENV PYTHONPATH /usr/local/airflow/mhs
#CMD ["python", "mhs/setup.py", "install", "--user"]
#COPY . /usr/local/airflow/mhs

#USER airflow

#ENTRYPOINT ["/entrypoint.sh"]
#CMD ["webserver"] # set default arg for entrypoint