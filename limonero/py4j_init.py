import sys

from py4j.java_gateway import JavaGateway


def init_jvm(flask_app):
    cp = {
        '/opt/hadoop-2.6.4/share/hadoop/common': [
            'hadoop-common-2.6.4.jar',
        ],
        '/opt/hadoop-2.6.4/share/hadoop/common/lib': [
            'commons-logging-1.1.3.jar',
            'guava-11.0.2.jar',
            'commons-collections-3.2.2.jar',
            'commons-configuration-1.6.jar',
            'commons-lang-2.6.jar',
            'hadoop-auth-2.6.4.jar',
            'slf4j-api-1.7.5.jar',
            'slf4j-log4j12-1.7.5.jar',
            'log4j-1.2.17.jar',
            'commons-cli-1.2.jar',
            'protobuf-java-2.5.0.jar',
            'htrace-core-3.0.4.jar',
            ''
        ],
        '/opt/hadoop-2.6.4/share/hadoop/hdfs': [
            'hadoop-hdfs-2.6.4.jar'
        ]
    }
    final_cp = ['{}/{}'.format(k, v) for k, values in cp.items() for v in
                values]

    flask_app.gateway = JavaGateway.launch_gateway(
        classpath=":".join(final_cp), redirect_stdout=sys.stdout,
        redirect_stderr=sys.stderr, die_on_exit=True)
