FROM ubuntu:18.04
ENV LIMONERO_HOME=/usr/local/limonero
ENV LIMONERO_CONFIG=${LIMONERO_HOME}/conf/limonero-config.yaml \
    PYTHONPATH=${PYTHONPATH}:${JUICER_HOME}

RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 \
      python3-pip \
      openjdk-8-jdk \
      locales \
  && update-alternatives --install /usr/bin/python python /usr/bin/python3.6 10 \
  && sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen \
  && locale-gen \
  && update-locale LANG=en_US.UTF-8 \
  && echo "LANG=en_US.UTF-8" >> /etc/default/locale \
  && echo "LANGUAGE=en_US.UTF-8" >> /etc/default/locale \
  && echo "LC_ALL=en_US.UTF-8" >> /etc/default/locale \
  && rm -rf /var/lib/apt/lists/*

WORKDIR $LIMONERO_HOME

COPY requirements.txt
RUN pip install -r requirements.txt

# Java dependencies.
ARG IVY_VERSION=2.3.0
ARG IVY_PKG=ivy-${IVY_VERSION}.jar
ARG IVY_URL=http://search.maven.org/remotecontent?filepath=org/apache/ivy/ivy/${IVY_VERSION}/${IVY_PKG}

COPY ivy.xml ./
RUN wget --quiet --directory-prefix /tmp $IVY_URL \
  && java -jar /tmp/${IVY_PKG} -retrieve "${LIMONERO_HOME}/jars/[artifact]-[revision](-[classifier]).[ext]" \
  && rm /tmp/${IVY_PKG}

COPY . $LIMONERO_HOME
RUN pybabel compile -d $LIMONERO_HOME/limonero/i18n/locales
CMD ["/usr/local/limonero/sbin/limonero-daemon.sh", "docker"]
