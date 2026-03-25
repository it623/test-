#!/bin/sh
while [ 1 ]
do
    nc -l -p 9100 | lpr -l -P "$1"
done

