#!/bin/bash

## Sets up a secure DB tunnel via AWS SSM.
##
## Usage:
## There's are many ways to connect DB
##   1. Manual    : bash ./before_local_launch.sh
##   2. With 'sw' : sw run {profile} -- ./before_local_launch.sh
##   3. IDE Setup : Add second script to the 'Before Launch' tasks in IntelliJ or other IDEs.


PROFILE=${1:-"default"}
export AWS_PROFILE=$PROFILE
echo -e "\nChecking tunnel status for profile: [$PROFILE]..."

## 2. Configure Database Tunneling through SSM
DB_PORT=3306
REGION="ap-southeast-3"
BASTION_ID="i-09976a0596200a406"
RDS_ENDPOINT="doosan-erp-dev-mysql.cfwsiekwsg6f.ap-southeast-3.rds.amazonaws.com"

LOG_FILE="/tmp/gossm_error.log"

check_port() {
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
        netstat -ano | grep LISTENING | grep ":$DB_PORT"
    else
        lsof -i:$DB_PORT -stcp:LISTEN
    fi
}
PID_INFO=$(check_port)

delete_logfile() {
    if [ -f "$LOG_FILE" ]; then
        rm -f "$LOG_FILE" 2>/dev/null || echo "Note: Log file is in use, skipping deletion."
    fi
}

## Uncomment below to see active profile info.
#echo "Checking AWS Identity..."
#aws sts get-caller-identity

if [ -z "$PID_INFO" ]; then
    echo -e "\nPort $DB_PORT is free. Starting gossm tunnel..."

    unset AWS_PROFILE
    unset AWS_DEFAULT_PROFILE

    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
        nohup gossm fwdrem -r $REGION -t $BASTION_ID -z $DB_PORT -a $RDS_ENDPOINT -l $DB_PORT 2> "$LOG_FILE" > /dev/null &
    else
        gossm fwdrem -r $REGION -t $BASTION_ID -z $DB_PORT -a $RDS_ENDPOINT -l $DB_PORT > /dev/null 2> "$LOG_FILE" &
    fi

    echo -e "\nWaiting for tunnel to establish (10s)...\n"
    sleep 10

    if [ -z "$(check_port)" ]; then
        echo "Failed to start tunnel."
        echo "Check your AWS SSM permissions."
        echo -e "\n-------------------- ERROR LOG --------------------"
          cat "$LOG_FILE"
        echo "---------------------------------------------------------"
        delete_logfile
        exit 1
    else
        echo "✅ Tunnel started successfully!"
        delete_logfile
    fi
else
    echo "Tunnel is already running on port $DB_PORT. Skipping..."
fi