#!/usr/bin/env sh

# This script controls the limonero server daemon initialization, status reporting
# and termination
# TODO: rotate logs

usage="Usage: limonero-daemon.sh (start|docker|stop|status)"

# this sript requires the command parameter
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

# parameter option
cmd_option=$1

# if unset set limonero_home project root without ./sbin
export LIMONERO_HOME=${LIMONERO_HOME:-$(cd $(dirname $0)/..; pwd)}
echo ${LIMONERO_HOME}

# get log directory
export LIMONERO_LOG_DIR=${LIMONERO_LOG_DIR:-${LIMONERO_HOME}/logs}

# get pid directory
export LIMONERO_PID_DIR=${LIMONERO_PID_DIR:-/var/run}
mkdir -p ${LIMONERO_PID_DIR} ${LIMONERO_LOG_DIR}

# log and pid files
log=${LIMONERO_LOG_DIR}/limonero-server-${USER}-${HOSTNAME}.out
pid=${LIMONERO_PID_DIR}/limonero-server-${USER}.pid

case $cmd_option in

  (start)
    # set python path
    PYTHONPATH=${LIMONERO_HOME}:${PYTHONPATH} \
      python ${LIMONERO_HOME}/limonero/manage.py \
      db upgrade
    PYTHONPATH=${LIMONERO_HOME}:${PYTHONPATH} nohup -- \
      python ${LIMONERO_HOME}/limonero/runner/limonero_server.py \
      -c ${LIMONERO_HOME}/conf/limonero-config.yaml \
      >> $log 2>&1 < /dev/null &
    limonero_server_pid=$!

    # persist the pid
    echo $limonero_server_pid > $pid
    echo "Limonero server started, logging to $log (pid=$limonero_server_pid)"
    ;;

  (docker)
    trap "$0 stop" SIGINT SIGTERM
    # set python path
    PYTHONPATH=${LIMONERO_HOME}:${PYTHONPATH} \
      python ${LIMONERO_HOME}/limonero/manage.py \
      db upgrade
    if [ $? -eq 0 ]
    then
      echo "DB migration: successful"
    else
      echo "Error on DB migration"
      exit 1
    fi
    PYTHONPATH=${LIMONERO_HOME}:${PYTHONPATH} \
      python ${LIMONERO_HOME}/limonero/runner/limonero_server.py \
      -c ${LIMONERO_HOME}/conf/limonero-config.yaml &
    limonero_server_pid=$!

    # persist the pid
    echo $limonero_server_pid > $pid
    echo "Limonero server started, logging to $log (pid=$limonero_server_pid)"
    wait
    ;;

  (stop)
    if [ -f $pid ]; then
      TARGET_ID=$(cat $pid)
      if [ $(pgrep -o -f limonero_server) -eq ${TARGET_ID} ]; then
        echo "stopping limonero server, user=${USER}, hostname=${HOSTNAME}"
        kill -SIGTERM ${TARGET_ID} && rm -f $pid
      else
        echo "no limonero server to stop"
      fi
    else
      echo "no limonero server to stop"
    fi
    ;;

  (status)
    if [ -f $pid ]; then
      TARGET_ID=$(cat $pid)
      if [ $(pgrep -o -f limonero_server) -eq ${TARGET_ID} ]; then
        echo "limonero server is running (pid=${TARGET_ID})"
        exit 0
      else
        echo "$pid file is present (pid=${TARGET_ID}) but limonero server not running"
        exit 1
      fi
    else
      echo limonero server not running.
      exit 2
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;
esac
