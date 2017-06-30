import glob
import sys

import os
from py4j.java_gateway import JavaGateway


def init_jvm(flask_app, logger):
    hadoop_home = os.environ.get('HADOOP_HOME')
    spark_home = os.environ.get('SPARK_HOME')
    cp = {}

    if hadoop_home:
        cp.update({
            '{0}/share/hadoop/common'.format(hadoop_home): [
                'hadoop-common-*.jar',
            ],
            '{0}/share/hadoop/common/lib'.format(hadoop_home): [
                'commons-logging-*.jar',
                'guava-*.jar',
                'commons-collections-*.jar',
                'commons-configuration-*.jar',
                'commons-lang-*.jar',
                'hadoop-auth-*.jar',
                'slf4j-api-*.jar',
                'slf4j-log4j*.jar',
                'log4j-*.jar',
                'commons-cli-*.jar',
                'protobuf-java-*.jar',
                'htrace-core-*.jar',
            ],
            '{0}/share/hadoop/hdfs'.format(hadoop_home): [
                'hadoop-hdfs-*.jar'
            ]
        })
    if spark_home:
        cp.update({
            '{0}/jars'.format(spark_home): [
                'hadoop-common-*.jar',
                'commons-logging-*.jar',
                'guava-*.jar',
                'commons-collections-*.jar',
                'commons-configuration-*.jar',
                'commons-lang-*.jar',
                'hadoop-auth-*.jar',
                'slf4j-api-*.jar',
                'slf4j-log4j*.jar',
                'log4j-*.jar',
                'commons-cli-*.jar',
                'protobuf-java-*.jar',
                'htrace-core-*.jar',
                'hadoop-hdfs-*.jar'
            ]
        })
    if cp:
        final_cp = []
        for path, exprs in cp.items():
            for expr in exprs:
                final_cp.extend(glob.glob(os.path.join(path, expr)))

        if not final_cp:
            logger.warn('Hadoop JARs not found. Data source upload will not '
                        'work. Set HADOOP_HOME and/or SPARK_HOME environment '
                        'variables to the correct path.')
            flask_app.gateway = None
        else:
            flask_app.gateway = JavaGateway.launch_gateway(
                classpath=":".join(final_cp), redirect_stdout=sys.stdout,
                redirect_stderr=sys.stderr, die_on_exit=True)

