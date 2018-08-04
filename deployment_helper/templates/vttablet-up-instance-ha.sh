
# Variables used below would be assigned values above this line
BACKUP_PARAMS_S3="-backup_storage_implementation s3 -s3_backup_aws_region us-west-2 -s3_backup_storage_bucket vtlabs-vtbackup"
BACKUP_PARAMS_FILE="-backup_storage_implementation file -file_backup_storage_root ${BACKUP_DIR} -restore_from_backup"

export LD_LIBRARY_PATH=${VTROOT}/dist/grpc/usr/local/lib
export PATH=${VTROOT}/bin:${VTROOT}/.local/bin:${VTROOT}/dist/chromedriver:${VTROOT}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/usr/local/go/bin:/usr/local/mysql/bin

case "$MYSQL_FLAVOR" in
  "MySQL56")
    export EXTRA_MY_CNF=$VTROOT/config/mycnf/master_mysql56.cnf
    ;;
  "MariaDB")
    export EXTRA_MY_CNF=$VTROOT/config/mycnf/master_mariadb.cnf
    ;;
  *)
    echo "Please set MYSQL_FLAVOR to MySQL56 or MariaDB."
    exit 1
    ;;
esac

mkdir -p ${VTDATAROOT}/tmp
mkdir -p ${BACKUP_DIR}

echo "Starting vttablet for $ALIAS..."

$VTROOT/bin/vttablet \
    $TOPOLOGY_FLAGS \
    -log_dir $VTDATAROOT/tmp \
    -tablet-path $ALIAS \
    -tablet_hostname "$HOSTNAME" \
    -init_keyspace $KEYSPACE \
    -init_shard $SHARD \
    -init_tablet_type $TABLET_TYPE \
    -init_db_name_override $DBNAME \
    -mycnf_mysql_port $MYSQL_PORT \
    -health_check_interval 5s \
    $BACKUP_PARAMS_FILE \
    -binlog_use_v3_resharding_mode \
    -port $WEB_PORT \
    -grpc_port $GRPC_PORT \
    -service_map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
    -pid_file $VTDATAROOT/$TABLET_DIR/vttablet.pid \
    -vtctld_addr http://${VTCTLD_HOST}:${VTCTLD_WEB_PORT}/ \
    -orc_api_url http://ec2-34-208-78-29.us-west-2.compute.amazonaws.com:3000/messagedb.0/api \
    -orc_discover_interval "2m" \
    $DBCONFIG_FLAGS \
    ${MYSQL_AUTH_PARAM} ${EXTRA_PARAMS}\
    > $VTDATAROOT/$TABLET_DIR/vttablet.out 2>&1 &

echo "Access tablet $ALIAS at http://$HOSTNAME:$WEB_PORT/debug/status"
