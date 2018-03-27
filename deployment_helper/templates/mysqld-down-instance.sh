
# Variables used below would be assigned values above this line

echo "Stopping MySQL for tablet $ALIAS..."
$VTROOT/bin/mysqlctl \
    $DBCONFIG_DBA_FLAGS \
    -tablet_uid $UNIQUE_ID \
    shutdown &

# Wait for 'mysqlctl shutdown' commands to finish.
wait
