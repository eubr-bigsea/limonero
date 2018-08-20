FROM python:2.7-alpine as pip_build
RUN apk add --no-cache g++
COPY requirements.txt /
RUN pip install -r /requirements.txt

FROM openjdk:8-jre-alpine
LABEL maintainer="Vinicius Dias <viniciusvdias@dcc.ufmg.br>, Guilherme Maluf \
<guimalufb@gmail.com>, Gabriel Barbutti <gabrielbarbutti@gmail.com>"

ENV LIMONERO_HOME=/usr/local/limonero 
ENV LIMONERO_CONFIG=$LIMONERO_HOME/conf/limonero-config.yaml \
    PYTHONPATH=$PYTHONPATH:$JUICER_HOME

COPY --from=pip_build /usr/local /usr/local

# Java dependencies
RUN curl -L -O http://search.maven.org/remotecontent?filepath=org/apache/ivy/ivy/2.3.0/ivy-2.3.0.jar && \
    java -jar ivy-2.3.0.jar -retrieve "jars/[artifact]-[revision](-[classifier]).[ext]" && \
    rm ivy-2.3.0.jar 

WORKDIR $LIMONERO_HOME
COPY . $LIMONERO_HOME

CMD ["/usr/local/limonero/sbin/limonero-daemon.sh", "docker"]
