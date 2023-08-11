FROM ubuntu:20.04
ARG DEBIAN_FRONTEND=noninteractive
ENV LIMONERO_HOME=/usr/local/limonero
ENV LIMONERO_CONFIG=${LIMONERO_HOME}/conf/limonero-config.yaml \
    PYTHONPATH=${PYTHONPATH}:${LIMONERO_HOME}

ENV HADOOP_VERSION_BASE=2.7.7
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
ENV HADOOP_HOME /opt/hadoop-$HADOOP_VERSION_BASE
ENV LD_LIBRARY_PATH="$HADOOP_HOME/lib/native/"

RUN apt-get update && apt-get install -y --no-install-recommends \
      libsasl2-dev \
      build-essential \
      python3 \
      python3-pip \
      python3-setuptools \
      openjdk-8-jdk \
      locales \
      netbase \
      git \
      wget \
      python3-dev \
      curl \
      dumb-init \ 
      sasl2-bin libsasl2-2 libsasl2-dev libsasl2-modules \
  && update-alternatives --install /usr/bin/python python /usr/bin/python3.8 10 \
  && sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen \
  && locale-gen \
  && update-locale LANG=en_US.UTF-8 \
  && echo "LANG=en_US.UTF-8" >> /etc/default/locale \
  && echo "LANGUAGE=en_US.UTF-8" >> /etc/default/locale \
  && echo "LC_ALL=en_US.UTF-8" >> /etc/default/locale \
  && rm -rf /var/lib/apt/lists/*

RUN curl -sL https://archive.apache.org/dist/hadoop/core/hadoop-${HADOOP_VERSION_BASE}/hadoop-${HADOOP_VERSION_BASE}.tar.gz | tar -xz -C /opt/ 
ENV PATH="$HADOOP_HOME/bin/:$PATH"
RUN echo 'export CLASSPATH=$(hadoop classpath --glob):$CLASSPATH' >> /etc/profile.d/hadoop-env.sh &&\
	chmod a+x /etc/profile.d/hadoop-env.sh

WORKDIR $LIMONERO_HOME

# Java dependencies.

COPY requirements.txt ./
RUN pip3 install -U pip wheel
RUN pip3 install -r requirements.txt

COPY . $LIMONERO_HOME
COPY ./conf/hdfs-site.xml $HADOOP_HOME/etc/hadoop/
RUN pybabel compile -d $LIMONERO_HOME/limonero/i18n/locales

COPY bin/entrypoint /usr/local/bin/

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/usr/local/bin/entrypoint"]
CMD ["server"]
# CMD ["/usr/local/limonero/sbin/limonero-daemon.sh", "docker"]
