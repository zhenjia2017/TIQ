#!/usr/bin/bash

## check argument length
if [[ $# -lt 1 ]]
then
	echo "Error: Invalid number of options: Please specify at least the pipeline-function."
	echo "Usage: bash scripts/pipeline.sh\\
		--year-page-retrieve\\
		/--pseudoquestion-generate\\
		/--question-rephrase\\
		[<PATH_TO_CONFIG>]"
	exit 0
fi

## read config parameter: if no present, stick to default (default.yaml)
FUNCTION=$1
CONFIG=${2:-"config/config-tiq.yml"}

## set path for output
# get function name
FUNCTION_NAME=${FUNCTION#"--"}
# get data name
IFS='/' read -ra NAME <<< "$CONFIG"
# get config name
CFG_NAME=${NAME[1]%".yml"}

# set output path (include sources only if not default value)
if [[ $# -lt 3 ]]
then
	OUT="out/pipeline-${FUNCTION_NAME}-${CFG_NAME}.out"
fi

echo $OUT

## start script
export FUNCTION CONFIG SOURCES OUT
nohup sh -c 'python -u tiq/pipeline.py $FUNCTION $CONFIG ' > $OUT 2>&1 &
