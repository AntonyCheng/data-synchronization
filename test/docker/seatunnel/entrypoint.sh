#!/bin/sh
set -eu

mkdir -p /opt/seatunnel/checkpoint /opt/seatunnel/logs /opt/seatunnel/poc-results

exec sh /opt/seatunnel/bin/seatunnel-cluster.sh
