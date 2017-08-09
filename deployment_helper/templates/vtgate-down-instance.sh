
# This script stops vtgate.

set -e

pid=`cat $VTDATAROOT/tmp/vtgate.pid`
echo "Stopping vtgate..."
kill $pid
