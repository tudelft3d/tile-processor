#!/bin/bash

# Allocate a constant amount or RAM for a user defined time.
# This script is used for testing the monitoring of a Worker's subprocess memory and CPU usage.
# It allocates around 580Mb or RAM.
# Reference: https://stackoverflow.com/a/4972220

echo "Provide sleep time in the form of NUMBER[SUFFIX]"
echo "   SUFFIX may be 's' for seconds (default), 'm' for minutes,"
echo "   'h' for hours, or 'd' for days."
delay=$1

echo "begin allocating memory..."
for index in $(seq 1000); do
    value=$(seq -w -s '' $index $(($index + 100000)))
    eval array$index=$value
done
echo "...end allocating memory"

echo "sleeping for $delay"
sleep $delay
