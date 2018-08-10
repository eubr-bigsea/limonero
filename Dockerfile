FROM python:2.7-alpine as pip_build
RUN apk add --no-cache g++
COPY requirements.txt /
RUN pip install -r /requirements.txt

FROM openjdk:8-jre-alpine
LABEL maintainer="Vinicius Dias <viniciusvdias@dcc.ufmg.br>, Guilherme Maluf \
<guimalufb@gmail.com>, Gabriel Barbutti <gabrielbarbutti@gmail.com>"

ARG SPARK_VERSION=2.3.1
ARG HADOOP_VERSION=2.7
ARG SPARK_HADOOP_PKG=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
ARG SPARK_HADOOP_URL=http://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_HADOOP_PKG}.tgz

ENV LIMONERO_HOME=/usr/local/limonero \
    SPARK_HOME=/usr/local/spark
ENV LIMONERO_CONFIG=$LIMONERO_HOME/conf/limonero-config.yaml \
    PYTHONPATH=$PYTHONPATH:$JUICER_HOME:$SPARK_HOME/python

RUN wget ${SPARK_HADOOP_URL} -O- | tar -xz -C /usr/local/ \
    && mv /usr/local/$SPARK_HADOOP_PKG $SPARK_HOME

COPY --from=pip_build /usr/local /usr/local

# Java dependencies
RUN mkdir -p $LIMONERO_HOME/jars \
   && cat java_libs.dep | xargs --max-args 1  wget --quiet --directory-prefix $LIMONERO_HOME/jars
ENV CLASSPATH $LIMONERO_HOME/jars/*.jar

WORKDIR $LIMONERO_HOME
COPY . $LIMONERO_HOME

CMD ["/usr/local/limonero/sbin/limonero-daemon.sh", "docker"]
