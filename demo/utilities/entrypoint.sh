#!/usr/bin/env bash
if [[ "$DEMO_PORT" == "" ]]; then
   echo "Error: environment variable DEMO_PORT is required"
   exit 1
fi

echo "Starting Gunicorn Web Server"

cd /app
gunicorn --bind 0.0.0.0:$DEMO_PORT app:app

