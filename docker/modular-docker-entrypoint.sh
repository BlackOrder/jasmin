#!/bin/bash
set -e

# Clean lock files
echo 'Cleaning lock files'
rm -f /tmp/*.lock

function startJasmind {
  echo 'Starting Jasmin Daemon'
  exec jasmind.py --enable-interceptor-client -u "${JCLI_ADMIN_USERNAME:-jcliadmin}" -p "${JCLI_ADMIN_PASSWORD_PLAIN:-jclipwd}"
}

function startDlrService {
  if [ "$1" = "Daemon" ]; then
    echo 'Starting DLRThrower Daemon'
    exec dlrd.py
  else
    echo 'Starting DLRThrower'
    dlrd.py &
  fi
}

function startDlrLookupService {
  if [ "$1" = "Daemon" ]; then
    echo 'Starting DLRLookup Daemon'
    exec dlrlookupd.py
  else
    echo 'Starting DLRLookup'
    dlrlookupd.py &
  fi
}

function startDeliverSmService {
  if [ "$1" = "Daemon" ]; then
    echo 'Starting DeliverSm Daemon'
    exec deliversmd.py
  else
    echo 'Starting DeliverSm'
    deliversmd.py &
  fi
}

function startInterceptorService {
  if [ "$1" = "Daemon" ]; then
    echo 'Starting Interceptor Server Daemon'
    exec interceptord.py
  else
    echo 'Starting Interceptor Server'
    interceptord.py &
  fi
}

function startRestApiService {
  if [ "$1" = "Daemon" ]; then
    echo 'Starting RestAPI Server Daemon'
    exec gunicorn -b 0.0.0.0:8080 jasmin.protocols.rest:api  --access-logfile '-' --error-logfile '-' --disable-redirect-access-to-syslog
  else
    echo 'Starting RestAPI Server'
    gunicorn -b 0.0.0.0:8080 jasmin.protocols.rest:api  --access-logfile '-' --error-logfile '-' --disable-redirect-access-to-syslog &
  fi
}

function startRestApiCelery {
  if [ "$1" = "Daemon" ]; then
    echo 'Starting RestAPI Celery Worker Daemon'
    exec celery -A jasmin.protocols.rest.tasks worker -l "${CELERY_LOG_LEVEL:-INFO}" -c 4 --autoscale=10,3
  else
    echo 'Starting RestAPI Celery Worker'
    celery -A jasmin.protocols.rest.tasks worker -l "${CELERY_LOG_LEVEL:-INFO}" -c 4 --autoscale=10,3 &
  fi
}

function startAll {
  startDlrService
  startDlrLookupService
  startDeliverSmService
  startInterceptorService
  startRestApiService
  startRestApiCelery
  startJasmind
}

# If Daemon is set to 'all', start all services
if [ "$Daemon" = "all" ]; then
  startAll
elif [ "$Daemon" = "jasmind" ]; then
  startJasmind "Daemon"
elif [ "$Daemon" = "dlrd" ]; then
  startDlrService "Daemon"
elif [ "$Daemon" = "dlrlookupd" ]; then
  startDlrLookupService "Daemon"
elif [ "$Daemon" = "deliversmd" ]; then
  startDeliverSmService "Daemon"
elif [ "$Daemon" = "interceptord" ]; then
  startInterceptorService "Daemon"
elif [ "$Daemon" = "restapi" ]; then
  startRestApiService "Daemon"
elif [ "$Daemon" = "celery" ]; then
  startRestApiCelery "Daemon"
fi

exec "$@"
