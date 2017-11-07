FROM python:3.6-stretch
RUN apt-get update && apt-get install -y \ 
	ksh \
	unzip \
	default-jre \
&& rm -rf /var/lib/apt/lists/*

COPY mv_to_docker/ibm_data_server_driver_package_linuxx64_v11.1.tar.gz /tmp/ibm_data_server_driver_package_linuxx64_v11.1.tar.gz
RUN tar -zxf /tmp/ibm_data_server_driver_package_linuxx64_v11.1.tar.gz -C /tmp
ENV CLASSPATH /tmp/dsdriver/java/db2jcc.jar
RUN /bin/ksh /tmp/dsdriver/installDSDriver

COPY mv_to_docker/requirements_jdbc.txt /tmp/requirements.txt
RUN pip install pip --upgrade
RUN pip install setuptools --upgrade
RUN pip install -r /tmp/requirements.txt
COPY . /code
RUN pip install -e /code
WORKDIR /code/ibmdbpy/tests
