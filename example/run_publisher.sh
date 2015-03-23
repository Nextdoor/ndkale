#!/bin/bash

INPUT=${1:-10}
export KALE_SETTINGS_MODULE=taskworker.settings
python publisher.py -n $INPUT
