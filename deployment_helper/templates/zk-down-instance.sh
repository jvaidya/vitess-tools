
# Variables used below would be assigned values above this line

mkdir -p $VTDATAROOT/tmp

action='shutdown'

$VTROOT/bin/zkctl -zk.myid $ZK_ID -zk.cfg $ZK_CONFIG -log_dir $VTDATAROOT/tmp $action \
		  > $VTDATAROOT/tmp/zkctl_$ZK_ID.out 2>&1 &

if ! wait $!; then
    echo "ZK server number $ZK_ID failed to stop. See log:"
    echo "    $VTDATAROOT/tmp/zkctl_$ZK_ID.out"
else
    echo "Stopped zk server $ZK_ID"
fi
