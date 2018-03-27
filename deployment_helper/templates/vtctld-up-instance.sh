

echo "Starting vtctld..."

mkdir -p $VTDATAROOT/backups
mkdir -p $VTDATAROOT/tmp

${VTROOT}/bin/vtctld \
  ${TOPOLOGY_FLAGS} \
  -cell ${CELL} \
  -web_dir ${VTTOP}/web/vtctld \
  -web_dir2 ${VTTOP}/web/vtctld2/app \
  -workflow_manager_init \
  -workflow_manager_use_election \
  -service_map 'grpc-vtctl' \
  -backup_storage_implementation file \
  -file_backup_storage_root ${VTDATAROOT}/backups \
  -log_dir ${VTDATAROOT}/tmp \
  -port ${WEB_PORT} \
  -grpc_port ${GRPC_PORT} \
  -pid_file ${VTDATAROOT}/tmp/vtctld.pid \
  ${MYSQL_AUTH_PARAM} \
  > ${VTDATAROOT}/tmp/vtctld.out 2>&1 &

disown -a


echo "Access vtctld web UI at http://${HOSTNAME}:${WEB_PORT}"
echo "Send commands with: vtctlclient -server ${HOSTNAME}:${GRPC_PORT} ..."
echo Note: vtctld writes logs under $VTDATAROOT/tmp.
