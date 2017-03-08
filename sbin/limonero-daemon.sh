#!/usr/bin/env bash

# This script controls the limonero server daemon initialization, status reporting
# and termination
# TODO: rotate logs

usage="Usage: limonero-daemon.sh (start|startf|stop|status)"

# this sript requires the command parameter
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

# parameter option
cmd_option=$1

# set limonero_home if unset
if [ -z "${LIMONERO_HOME}" ]; then
  export LIMONERO_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi
echo $LIMONERO_HOME

# get log directory
if [ "$LIMONERO_LOG_DIR" = "" ]; then
  export LIMONERO_LOG_DIR="${LIMONERO_HOME}/logs"
fi
mkdir -p "$LIMONERO_LOG_DIR"

# get pid directory
if [ "$LIMONERO_PID_DIR" = "" ]; then
  export LIMONERO_PID_DIR=/tmp
fi
mkdir -p "$LIMONERO_PID_DIR"

# log and pid files
log="$LIMONERO_LOG_DIR/limonero-server-$USER-$HOSTNAME.out"
pid="$LIMONERO_PID_DIR/limonero-server-$USER.pid"

case $cmd_option in

   (start)
      # set python path
      PYTHONPATH=$LIMONERO_HOME:$PYTHONPATH nohup -- python $LIMONERO_HOME/limonero/runner/limonero_server.py \
         -c $LIMONERO_HOME/conf/limonero-config.yaml >> $log 2>&1 < /dev/null &
      limonero_server_pid=$!

      # persist the pid
      echo $limonero_server_pid > $pid

      echo "Limonero server started, logging to $log (pid=$limonero_server_pid)"
      ;;

   (startf)
      trap "$0 stop" SIGINT SIGTERM
      $0 start
      sleep infinity &
      wait
      ;;

   (stop)

      if [ -f $pid ]; then
         TARGET_ID="$(cat "$pid")"
         if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "python" ]]; then
            echo "stopping limonero server, user=$USER, hostname=$HOSTNAME"
            kill -SIGTERM "$TARGET_ID" && rm -f "$pid"
         else
            echo "no limonero server to stop"
         fi
      else
         echo "no limonero server to stop"
      fi
      ;;

   (status)

      if [ -f $pid ]; then
         TARGET_ID="$(cat "$pid")"
         if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "python" ]]; then
            echo "limonero server is running (pid=$TARGET_ID)"
            exit 0
         else
            echo "$pid file is present (pid=$TARGET_ID) but limonero server not running"
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
