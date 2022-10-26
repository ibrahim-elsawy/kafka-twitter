#!/bin/sh

stop_master.sh;
stop_slave.sh;
start-master.sh; 
start-slave.sh spark://ubuntu:7077;