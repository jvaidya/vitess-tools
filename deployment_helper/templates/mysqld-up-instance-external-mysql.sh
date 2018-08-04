
# Variables used below would be assigned values above this line

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

echo "Starting MySQL for tablet $ALIAS..."

if [ $EXTERNAL_MYSQL -eq 0 ]; then

    if [ -d $VTDATAROOT/$TABLET_DIR ]; then
	echo "Resuming from existing vttablet dir:"
	echo "    $VTDATAROOT/$TABLET_DIR"
	action='start'
	$VTROOT/bin/mysqlctl \
	    -log_dir $VTDATAROOT/tmp \
	    -tablet_uid $UNIQUE_ID \
	    $DBCONFIG_DBA_FLAGS \
	    -mysql_port $MYSQL_PORT \
	    $action &
    else
	action="init -init_db_sql_file $INIT_DB_SQL_FILE"
	$VTROOT/bin/mysqlctl \
	    -log_dir $VTDATAROOT/tmp \
	    -tablet_uid $UNIQUE_ID \
	    $DBCONFIG_DBA_FLAGS \
	    -mysql_port $MYSQL_PORT \
	    $action &
    fi

else
    if [ ! -d $VTDATAROOT/$TABLET_DIR ]; then
	echo "Creating vttablet dir:"
	echo "    $VTDATAROOT/$TABLET_DIR"
	action='init_config'
	$VTROOT/bin/mysqlctl \
	    -log_dir $VTDATAROOT/tmp \
	    -tablet_uid $UNIQUE_ID \
	    $DBCONFIG_DBA_FLAGS \
	    -mysql_port $MYSQL_PORT \
	    $action
    fi
fi

wait
