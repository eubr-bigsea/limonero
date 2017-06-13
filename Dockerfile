FROM ubuntu:16.04
MAINTAINER Vinicius Dias <viniciusvdias@dcc.ufmg.br>

ENV LIMONERO_HOME /usr/local/limonero
ENV LIMONERO_CONFIG $LIMONERO_HOME/conf/limonero-config.yaml

ENV SPARK_HADOOP_PKG spark-2.1.0-bin-hadoop2.6
ENV SPARK_HADOOP_URL http://www-eu.apache.org/dist/spark/spark-2.1.0/${SPARK_HADOOP_PKG}.tgz
ENV SPARK_HOME /usr/local/spark
ENV PYTHONPATH $PYTHONPATH:$JUICER_HOME:$SPARK_HOME/python

RUN apt-get update && apt-get install -y  \
     python-pip \
     openjdk-8-jdk \
   && rm -rf /var/lib/apt/lists/*

WORKDIR $LIMONERO_HOME
COPY . $LIMONERO_HOME
RUN pip install -r $LIMONERO_HOME/requirements.txt

CMD ["/usr/local/limonero/sbin/limonero-daemon.sh", "startf"]
