
# Variables used below would be assigned values above this line

echo "Stopping MySQL for tablet $ALIAS..."
$VTROOT/bin/mysqlctl \
    -db-config-dba-uname vt_dba \
    -tablet_uid $UNIQUE_ID \
    shutdown &

# Wait for 'mysqlctl shutdown' commands to finish.
wait
