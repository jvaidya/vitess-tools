
# Variables used below would be assigned values above this line

export LD_LIBRARY_PATH=/home/ubuntu/dist/grpc/usr/local/lib
export PATH=/home/ubuntu/bin:/home/ubuntu/.local/bin:/home/ubuntu/dist/chromedriver:/home/ubuntu/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/usr/local/go/bin:/usr/local/mysql/bin

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
mkdir -p ${VTDATAROOT}/backups

echo "Starting MySQL for tablet $ALIAS..."
action="init -init_db_sql_file $INIT_DB_SQL_FILE"

if [ -d $VTDATAROOT/$TABLET_DIR ]; then
    echo "Resuming from existing vttablet dir:"
    echo "    $VTDATAROOT/$TABLET_DIR"
    action='start'
fi

$VTROOT/bin/mysqlctl \
    -log_dir $VTDATAROOT/tmp \
    -tablet_uid $UNIQUE_ID \
    $DBCONFIG_DBA_FLAGS \
    -mysql_port $MYSQL_PORT \
    $action &

wait

echo "Starting vttablet for $ALIAS..."

$VTROOT/bin/vttablet \
    $TOPOLOGY_FLAGS \
    -log_dir $VTDATAROOT/tmp \
    -tablet-path $ALIAS \
    -tablet_hostname "$HOSTNAME" \
    -init_keyspace $KEYSPACE \
    -init_shard $SHARD \
    -init_tablet_type $TABLET_TYPE \
    -health_check_interval 5s \
    -enable_semi_sync \
    -enable_replication_reporter \
    -backup_storage_implementation file \
    -file_backup_storage_root $VTDATAROOT/backups \
    -restore_from_backup \
    -binlog_use_v3_resharding_mode \
    -port $WEB_PORT \
    -grpc_port $GRPC_PORT \
    -service_map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
    -pid_file $VTDATAROOT/$TABLET_DIR/vttablet.pid \
    -vtctld_addr http://${VTCTLD_HOST}:${VTCTLD_WEB_PORT}/ \
    $DBCONFIG_FLAGS -v 3 \
    > $VTDATAROOT/$TABLET_DIR/vttablet.out 2>&1 &

echo "Access tablet $ALIAS at http://$HOSTNAME:$WEB_PORT/debug/status"
