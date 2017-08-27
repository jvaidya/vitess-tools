
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


