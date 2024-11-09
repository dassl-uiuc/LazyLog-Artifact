#!/bin/bash

pname=$1

sudo pkill -2 $pname 
sleep 2
if sudo pgrep -f $pname > /dev/null; 
then 
    sudo pkill -9 $pname
fi