#!/bin/bash

# Just print the processed tile $1 into a file $2

FILE=$1
TILE=$2

if [ -f $FILE ]; then
   rm $FILE
   touch $FILE
else
   touch $FILE
fi

sleep 1s
echo $(date)"\tDone with tile "$TILE".\n" >> $FILE
