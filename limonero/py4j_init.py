import glob
import os
import sys

from py4j.java_gateway import JavaGateway, GatewayParameters, launch_gateway


def init_jvm(flask_app, logger):
    flask_app.gateway_port = create_jvm(logger)


def create_jvm(logger):
    port = None
    cp = {'lib': ['*.jar'], 'jars': ['*.jar']}
    final_cp = []
    for dist_dir in [os.getenv('HADOOP_HOME'), os.getenv('SPARK_HOME')]:
        if dist_dir:
            for path, exprs in list(cp.items()):
                for expr in exprs:
                    final_cp.extend(
                        glob.glob(os.path.join(dist_dir, path, expr)))
    if not final_cp:
        logger.warn('Hadoop JARs not found. Data source upload will not '
                    'work. Set HADOOP_HOME and/or SPARK_HOME environment '
                    'variables to the correct path.')
    else:
        final_cp = [os.path.abspath(p) for p in final_cp]
        port = launch_gateway(classpath=":".join(final_cp),
                              redirect_stdout=sys.stdout,
                              redirect_stderr=sys.stderr, die_on_exit=True)
    return port


def create_gateway(logger, port):
    params = GatewayParameters(port=port, eager_load=True)
    return JavaGateway(gateway_parameters=params)
