FROM ubuntu:16.04
MAINTAINER Vinicius Dias <viniciusvdias@dcc.ufmg.br>

ENV LIMONERO_HOME /usr/local/limonero
ENV LIMONERO_CONFIG $LIMONERO_HOME/conf/limonero-config.yaml

RUN apt-get update && apt-get install -y  \
     python-pip \
   && rm -rf /var/lib/apt/lists/*

WORKDIR $LIMONERO_HOME
COPY . $LIMONERO_HOME
RUN pip install -r $LIMONERO_HOME/requirements.txt

CMD ["/usr/local/limonero/sbin/limonero-daemon.sh", "startf"]
