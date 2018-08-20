import glob
import os
import sys

from py4j.java_gateway import JavaGateway, GatewayParameters, launch_gateway


def init_jvm(flask_app, logger):
    flask_app.gateway_port = create_jvm(logger)


def create_jvm(logger):
    cp = {}
    port = None

    cp.update({'/tmp/ivy/lib': ['*.jar']})

    if cp:
        final_cp = []
        for path, exprs in cp.items():
            for expr in exprs:
                final_cp.extend(glob.glob(os.path.join(path, expr)))

        if not final_cp:
            logger.warn('Hadoop JARs not found. Data source upload will not '
                        'work. Set HADOOP_HOME and/or SPARK_HOME environment '
                        'variables to the correct path.')
        else:
            port = launch_gateway(classpath=":".join(final_cp),
                                  redirect_stdout=sys.stdout,
                                  redirect_stderr=sys.stderr, die_on_exit=True)
    return port


def create_gateway(logger, port):
    params = GatewayParameters(port=port, eager_load=True)
    return JavaGateway(gateway_parameters=params)
