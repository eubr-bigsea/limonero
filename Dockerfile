FROM conda/miniconda2
LABEL maintainer="Vinicius Dias <viniciusvdias@dcc.ufmg.br>, \
  Guilherme Maluf <guimalufb@gmail.com>, \
  Michel Boaventura <michel.boaventura@gmail.com>"

ENV LIMONERO_HOME /usr/local/limonero
ENV LIMONERO_CONFIG $LIMONERO_HOME/conf/limonero-config.yaml

ENV PYTHONPATH $PYTHONPATH:$JUICER_HOME

RUN conda install hdfs3 -y -c conda-forge

WORKDIR $LIMONERO_HOME
COPY requirements.txt $LIMONERO_HOME/requirements.txt
RUN pip install -r $LIMONERO_HOME/requirements.txt
COPY . $LIMONERO_HOME

CMD ["/usr/local/limonero/sbin/limonero-daemon.sh", "startf"]
