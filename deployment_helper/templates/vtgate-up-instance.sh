
mkdir -p $VTDATAROOT/tmp
mkdir -p $VTDATAROOT/backups

# Start vtgate.
$VTROOT/bin/vtgate \
  $TOPOLOGY_FLAGS \
  -log_dir $VTDATAROOT/tmp \
  -port ${WEB_PORT} \
  -grpc_port ${GRPC_PORT} \
  -mysql_server_port ${MYSQL_SERVER_PORT} \
  -mysql_auth_server_static_string '{"mysql_user":{"Password":"mysql_password"}}' \
  -cell ${CELL} \
  -cells_to_watch ${CELL} \
  -tablet_types_to_wait MASTER,REPLICA \
  -gateway_implementation discoverygateway \
  -service_map 'grpc-vtgateservice' \
  -pid_file $VTDATAROOT/tmp/vtgate.pid \
  > $VTDATAROOT/tmp/vtgate.out 2>&1 &

echo "Access vtgate at http://${HOSTNAME}:${WEB_PORT}/debug/status"
echo Note: vtgate writes logs under $VTDATAROOT/tmp.

disown -a
