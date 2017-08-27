
# Variables used below would be assigned values above this line

echo "Stopping vttablet for $ALIAS..."
pid=`cat $VTDATAROOT/$TABLET_DIR/vttablet.pid`
kill $pid

while ps -p $pid > /dev/null; do sleep 1; done

