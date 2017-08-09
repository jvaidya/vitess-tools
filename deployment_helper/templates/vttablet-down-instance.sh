
# Variables used below would be assigned values above this line

echo "Stopping vttablet for $ALIAS..."
pid=`cat $VTDATAROOT/$TABLET_DIR/vttablet.pid`
kill $pid

echo "Stopping MySQL for tablet $ALIAS..."
$VTROOT/bin/mysqlctl \
    -db-config-dba-uname vt_dba \
    -tablet_uid $UNIQUE_ID \
    shutdown &

# Wait for vttablet to die.
while ps -p $pid > /dev/null; do sleep 1; done

# Wait for 'mysqlctl shutdown' commands to finish.
wait
