FROM python:2.7-alpine as pip_build
RUN apk add --no-cache g++
COPY requirements.txt /
RUN pip install -r /requirements.txt

FROM openjdk:8-jre-alpine
LABEL maintainer="Vinicius Dias <viniciusvdias@dcc.ufmg.br>, Guilherme Maluf \
<guimalufb@gmail.com>, Gabriel Barbutti <gabrielbarbutti@gmail.com>"

ENV LIMONERO_HOME=/usr/local/limonero \
    LIMONERO_CONFIG=${LIMONERO_HOME}/conf/limonero-config.yaml \
    PYTHONPATH=${PYTHONPATH}:${JUICER_HOME}

COPY --from=pip_build /usr/local /usr/local

WORKDIR $LIMONERO_HOME

# Java dependencies
ARG IVY_VERSION=2.3.0
ARG IVY_PKG=ivy-${IVY_VERSION}.jar
ARG IVY_URL=http://search.maven.org/remotecontent?filepath=org/apache/ivy/ivy/${IVY_VERSION}/${IVY_PKG}

COPY ivy.xml ./
RUN wget --quiet --directory-prefix /tmp $IVY_URL \
  && java -jar /tmp/${IVY_PKG} -retrieve "${LIMONERO_HOME}/jars/[artifact]-[revision](-[classifier]).[ext]" \
  && rm /tmp/${IVY_PKG}

COPY . $LIMONERO_HOME
CMD ["/usr/local/limonero/sbin/limonero-daemon.sh", "docker"]
