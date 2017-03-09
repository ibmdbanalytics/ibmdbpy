FROM python:2.7
RUN apt-get update && apt-get install -y \ 
	ksh \
	unzip \
	unixodbc \
	vim \
&& rm -rf /var/lib/apt/lists/*

COPY mv_to_docker/ibm_data_server_driver_package_linuxx64_v11.1.tar.gz /tmp/ibm_data_server_driver_package_linuxx64_v11.1.tar.gz
RUN tar -zxf /tmp/ibm_data_server_driver_package_linuxx64_v11.1.tar.gz -C /tmp
RUN /bin/ksh /tmp/dsdriver/installDSDriver

COPY mv_to_docker/odbc.ini /etc/odbc.ini
COPY mv_to_docker/odbcinst.ini /etc/odbcinst.ini
COPY mv_to_docker/db2dsdriver.cfg /tmp/dsdriver/cfg/db2dsdriver.cfg

COPY mv_to_docker/requirements_27_odbc.txt /tmp/requirements.txt
RUN pip install pip --upgrade
RUN pip install setuptools --upgrade
RUN pip install -r /tmp/requirements.txt
COPY . /code
RUN pip install -e /code
WORKDIR /code/ibmdbpy/tests
