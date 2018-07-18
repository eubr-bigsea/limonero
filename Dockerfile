FROM openjdk:8-jre
LABEL maintainer="Vinicius Dias <viniciusvdias@dcc.ufmg.br>, Guilherme Maluf <guimalufb@gmail.com>"

ENV LIMONERO_HOME /usr/local/limonero
ENV LIMONERO_CONFIG $LIMONERO_HOME/conf/limonero-config.yaml

ENV SPARK_HOME /usr/local/spark
ENV PYTHONPATH $PYTHONPATH:$JUICER_HOME:$SPARK_HOME/python

ARG SPARK_VERSION=2.3.0
ARG HADOOP_VERSION=2.7
ARG SPARK_HADOOP_PKG=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
ARG SPARK_HADOOP_URL=http://www-eu.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_HADOOP_PKG}.tgz

RUN apt-get update && apt-get install -y curl \
   && curl -s ${SPARK_HADOOP_URL} | tar -xz -C /usr/local/  \
   && mv /usr/local/$SPARK_HADOOP_PKG $SPARK_HOME

RUN apt-get install -y  \
     python-pip \
   && rm -rf /var/lib/apt/lists/*

WORKDIR $LIMONERO_HOME
COPY requirements.txt $LIMONERO_HOME/requirements.txt
RUN pip install -r $LIMONERO_HOME/requirements.txt
COPY . $LIMONERO_HOME

# Java dependencies
ENV CLASSPATH $LIMONERO_HOME/jars/*.jar
RUN mkdir -p $LIMONERO_HOME/jars \
   && for u in `cat java_libs.dep`; do cd jars && curl -s -O $u && cd ..; done

CMD ["/usr/local/limonero/sbin/limonero-daemon.sh", "startf"]
