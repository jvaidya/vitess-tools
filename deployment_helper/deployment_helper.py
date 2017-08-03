import argparse
import socket
import urllib2
import os
import sys
import types
import readline
import json
import subprocess

DEPLOYMENT_DIR = os.path.expanduser('~/vitess-deployment')

"""
Goal 1: Create up and down scripts for localhost.
DEPLOYMENT_DIR/bin/
     zk-up.sh
     vtctld-up.sh
     vtgate-up.sh
     vttablet-up.sh
A config class that reads and writes from config.

"""

CFGFILE = os.path.join(os.environ['VTDATAROOT'], 'deployment.json')
VTROOT = None
VTDATAROOT = None
CELL = None

class ZkCell(object):
    num_hosts = 0


def check_host():
    global VTROOT, VTDATAROOT
    print """
We assume that you are running this on a host on which all the required vitess binaries exist
and you have set the VTROOT and VTDATAROOT environment variable.
VTROOT is the root of vitess installation. We expect to find the vitess binaries under VTROOT/bin and
config files under VTROOT/config. This program too stores it's data files under VTROOT/config.
VTDATAROOT is where the mysql data files, backup files, log files etc. are stored. This would typically be
on a partition where there is enough disk space for your data.
"""
    VTROOT = os.environ.get('VTROOT') or read_value('Did not find VTROOT in environment. Enter VTROOT:')
    VTDATAROOT = os.environ['VTDATAROOT'] or read_value('Did not find VTDATAROOT in environment. Enter VTDATAROOT:')
    print 'VTROOT=%s' % VTROOT
    print 'VTDATAROOT=%s' % VTDATAROOT
    print
    
g_local_hostname = socket.getfqdn()

def read_value(prompt, prefill=''):
    if not prompt.endswith(' '):
        prompt += ' '
    readline.set_startup_hook(lambda: readline.insert_text(prefill))
    try:
        return raw_input(prompt).strip()
    finally:
        readline.set_startup_hook()

def set_cell(default):
    global CELL
    CELL = os.environ.get('CELL') or read_value('Enter CELL name:', default)

base_ports = {
    'zk2': dict(leader_port=28881, election_port=38881, client_port=21811),
    'etcd': 2379,
    'vtctld': dict(port=15000, grpc_port=15999),
    'vttablet': dict(web_port=15101, grpc_port=16101, mysql_port=17101),
    'mysqld': 1000,
    'vtgate': dict(web_port=15001, grpc_port=15991, mysql_server_port=15306)
}

class ConfigType(object):
    """Each config variable has a default.
    It is read from a config file.
    Written to a config file.
    Is prompted for and read from user input.
    """
    ConfigTypes = [types.DictType, types.StringType, types.ListType, types.IntType]    

    def get_config_file(self):
        config_dir = os.path.join(DEPLOYMENT_DIR, 'config')
        return os.path.join(config_dir, '%s.json' % self.short_name)

    def write_config(self):
        config_file = self.get_config_file()
        if not os.path.exists(os.path.dirname(config_file)):
            os.makedirs(os.path.dirname(config_file))
        out = { k: self.__dict__[k] for k in self.__dict__ if type(self.__dict__[k]) in self.ConfigTypes }
        try:
            with open(config_file, 'w') as fh:
                json.dump(out, fh, indent=4, separators=(',', ': '))
        except Exception as e:
            print e
            print self.__dict__
            for k in self.__dict__:
                print '%s %s' % (k, type(self.__dict__[k]))
        #print 'Wrote config for "%s" to "%s"' % (self.short_name, config_file)

    def read_config(self):
        config_file = self.get_config_file()
        interactive = True
        if os.path.exists(config_file):
            use_file = read_value('Config file "%s" exists, use that? :' % config_file, 'Y')
            interactive = use_file != 'Y'
        if interactive:
            get_hosts = getattr(self, 'get_hosts', None)
            if callable(get_hosts):
                get_hosts()
            self.read_config_interactive()
            self.write_config()
        else:
            with open(config_file) as fh:
                self.__dict__.update(json.load(fh))            
        
# We need to keep a per-host, per port-type count of last port used
# So that we can properly increment when there are multiple instances
# on the same host

class HostClass(ConfigType):
    up_filename = None
    down_filename = None
    name = ""
    description = ""
    hardware_recommendation = ""
    host_number_calculation = ""
    num_recommended_hosts = 0
    available_hosts = []

    def read_config(self):
        self.prologue()
        super(HostClass, self).read_config()
        
    def prologue(self):
        name = self.name or self.short_name
        name = '  %s  ' % name
        print
        print name.center(80, '*')
        print
        if self.description:
            print
            print self.description
            print
        if self.hardware_recommendation:
            print self.hardware_recommendation
            print
            print self.host_number_calculation
            print
    
    def get_hosts(self):
        print """Please specify hosts to use for this component.
To specify hosts, you can enter hostnames seperated by commas or
you can specify a file (one host per line) as "file:/path/to/file"."""
        public_hostname = get_public_hostname()
        host_prompt = 'Specify hosts for "%s":' % self.short_name        
        host_input = read_value(host_prompt, public_hostname)
        if host_input.startswith('file:'):
            _, path = host_input.split(':')
            with open(path) as fh:
                self.available_hosts = fh.readlines()
        else:
            self.available_hosts = host_input.split(':')

    def read_config_interactive(self):
        raise NotImplemented

    def up_commands(self):
        raise NotImplemented

    def write_files(self):
        if self.up_filename:
            out = self.up_commands()
            write_bin_file(self.up_filename, out)
        if self.down_filename:
            out = self.down_commands()
            write_bin_file(self.down_filename, out)
        
class Deployment(object):
    pass

class LockServer(HostClass):
    short_name = 'lockserver'
    def __init__(self):
        self.ls = None
        self.ls_type = None
        
    def read_config_interactive(self):
        print 'Vitess supports two types of lockservers, zookeeper (zk2) and etcd (etcd)'
        print
        self.ls_type = read_value('Enter the type of lockserver you want to use {"zk2", "etcd"} :', 'zk2')
        print

    def read_config(self):
        super(LockServer, self).read_config()

        if self.ls_type != 'zk2':
            print >> sys.stderr, 'ERROR: Not Implemnted'
            sys.exit(1)

        self.ls = Zk2()
        self.ls.read_config()
        self.ls.set_topology()
        self.topology_flags = self.ls.topology_flags
        self.num_instances = self.ls.num_instances
        
    def write_files(self):
        self.ls.write_files()

    def set_topology_from_vtctld(self, cell_info):
        if '21811' in cell_info['server_address']:
            self.ls_type = 'zk2'
            self.server_var = cell_info['server_address']            
            self.topology_flags = ' '.join(['-topo_implementation %s' % self.ls_type,
                                       '-topo_global_server_address %s' % self.server_var,
                                       '-topo_global_root /vitess/global'])
        else:
            raise Exception('etcd not supported at this time.')

class Zk2(HostClass):
    up_filename = 'zk-up.sh'
    down_filename = 'zk-down.sh'
    name = 'Zookeeper (zk2)'
    short_name = 'zk2'
    description = 'Zookeeper is a popular open source lock server written in Java'
    hardware_recommendation = 'We recommend a host with x cpus and y memory for each Zookeeper instance'
    host_number_calculation = """A LockServer needs odd number of instances to establish quorum, we recommend at least 3
on 3 different hosts. If you are running the local cluster demo, you can run all three on one host."""
    
    def __init__(self):
        self.zk_hosts = []
        self.zk_config = []

    def get_default_host(self, i):
        return self.available_hosts[i % len(self.available_hosts)]
    
    def read_config_interactive(self):
        print
        self.num_instances = int(read_value('Enter number of instances :', '3'))
        
        for i in xrange(self.num_instances):
            instance_num = i + 1
            zk_host = read_value('For instance %d, enter hostname: ' % instance_num, self.get_default_host(i))
            self.zk_hosts.append(zk_host)
            leader_port = base_ports['zk2']['leader_port'] + self.zk_hosts.count(zk_host) - 1
            election_port = base_ports['zk2']['election_port'] + self.zk_hosts.count(zk_host) - 1
            client_port = base_ports['zk2']['client_port'] + self.zk_hosts.count(zk_host) - 1
            def_ports = '%(leader_port)s:%(election_port)s:%(client_port)s' % locals()
            zk_ports = read_value('For instance %d, enter leader_port:election_port:client_port: ' % instance_num, def_ports)
            print
            self.zk_config.append((zk_host,zk_ports))

    def set_topology(self):
        zk_cfg_lines = []
        zk_server_lines = []
        for i, (zk_host, zk_ports) in enumerate(self.zk_config):
            count = i + 1
            client_port = zk_ports.split(':')[-1]
            zk_cfg_lines.append('%(count)s@%(zk_host)s:%(zk_ports)s' %locals())
            zk_server_lines.append('%(zk_host)s:%(client_port)s' %locals())
        self.zk_server_var = ','.join(zk_server_lines)
        self.zk_config_var = ','.join(zk_cfg_lines)
        self.topology_flags = ' '.join(['-topo_implementation zk2',
                                       '-topo_global_server_address %s' % self.zk_server_var,
                                       '-topo_global_root /vitess/global'])
    def make_header(self):
        zk_config_var = self.zk_config_var
        topology_flags = self.topology_flags
        zk_server_var = self.zk_server_var
        cell = CELL
        return """#!/bin/bash

ZK_CONFIG="%(zk_config_var)s"
ZK_SERVER="%(zk_server_var)s"
TOPOLOGY_FLAGS="%(topology_flags)s"
CELL="%(cell)s"

mkdir -p ${VTDATAROOT}/tmp
""" % locals()

    def _make_zk_command(self, count):
        cmd = [os.path.join(VTROOT, 'bin/zkctl'),
               '-zk.myid', count,
               '-zk.cfg', '${ZK_CONFIG}',
               '-log_dir', os.path.join(VTDATAROOT, 'tmp'),
               '${ACTION}']
        return ' '.join(cmd)

    def down_commands(self):
        out = [self.make_header()]
        action = """
# Stop ZooKeeper servers.
echo "Stopping zk servers..."
ACTION="shutdown"
"""
        out.append(action)
        for i, (zk_host, zk_ports) in enumerate(self.zk_config):
            count = str(i + 1)
            cmd = self._make_zk_command(count)
            out.append('# Stop zk2 instance %s' % count)
            out.append(cmd)
            out.append('')

        return '\n'.join(out)

    def up_commands(self):
        out = [self.make_header()]
        out.append('echo "Starting zk servers..."')
        for i, (zk_host, zk_ports) in enumerate(self.zk_config):
            count = str(i + 1)
            test = """
if [ -e $VTDATAROOT/zk_%03d ]; then
    ACTION="start"
else
    ACTION="init"
fi""" % int(count)
            out.append(test)
            cmd = self._make_zk_command(count)            
            out.append('# Start instance %s' % count)
            out.append(cmd)

        out.append('')
        out.append('# Create /vitess/global and /vitess/CELLNAME paths if they do not exist.')
        cmd = [os.path.join(VTROOT, 'bin/zk'),
               '-server', '${ZK_SERVER}',
               'touch','-p','/vitess/global']
        
        out.append(' '.join(cmd))
        cmd = [os.path.join(VTROOT, 'bin/zk'),
               '-server', '${ZK_SERVER}',
               'touch','-p','/vitess/${CELL}']
        out.append(' '.join(cmd))

        out.append('')
        out.append('# Initialize cell.')
        cmd = [os.path.join(VTROOT, 'bin/vtctl'),
               self.topology_flags,
               'AddCellInfo',
               '-root /vitess/${CELL}',
               '-server_address', '${ZK_SERVER}',
               '${CELL}']
        out.append(' '.join(cmd))
        out.append('')
        return '\n'.join(out)

def write_bin_file(fname, out):
    write_dep_file('bin', fname, out)
    
def write_dep_file(subdir, fname, out):
    dirpath = os.path.join(DEPLOYMENT_DIR, subdir)
    if not os.path.isdir(dirpath):
        os.makedirs(dirpath)
    with open(os.path.join(dirpath, fname), 'w') as fh:
        fh.write(out)
    if subdir == 'bin':
        os.chmod(os.path.join(dirpath, fname), 0755)
        
class VtCtld(HostClass):
    name = 'VtCtld server'
    description = """The vtctld server provides a web interface that displays all of the coordination information stored in ZooKeeper.
The vtctld server also accepts commands from the vtctlclient tool, which is used to administer the cluster."""
    hardware_recommendation = 'We recommend a host with x cpus and y memory for the vtctld instance.'
    host_number_calculation = 'You typically need only 1 vtctld instance in a cluster.'
    up_filename = 'vtctld-up.sh'
    down_filename = 'vtctld-down.sh'
    short_name = 'vtctld'
    
    def __init__(self, hostname, ls):
        self.hostname = hostname
        self.ls = ls
        self.ports = dict(web_port=15000, grpc_port=15999)

    def read_config_interactive(self):
        pass

    def down_commands(self):
        return """#!/bin/bash

# This script stops vtctld.

set -e

pid=`cat $VTDATAROOT/tmp/vtctld.pid`
echo "Stopping vtctld..."
kill $pid
"""
    def up_commands(self):
        topology_flags = self.ls.topology_flags
        cell = CELL
        grpc_port = self.ports['grpc_port']
        web_port = self.ports['web_port']
        hostname = self.hostname
        return r"""
#!/bin/bash
set -e

HOSTNAME="%(hostname)s"
TOPOLOGY_FLAGS="%(topology_flags)s"
CELL="%(cell)s"
GRPC_PORT=%(grpc_port)s
WEB_PORT=%(web_port)s

echo "Starting vtctld..."

mkdir -p $VTDATAROOT/backups

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
  > ${VTDATAROOT}/tmp/vtctld.out 2>&1 &

disown -a

echo "Access vtctld web UI at http://${HOSTNAME}:${WEB_PORT}"
echo "Send commands with: vtctlclient -server ${HOSTNAME}:${GRPC_PORT} ..."
""" % locals()

class VtGate(HostClass):
    up_filename = 'vtgate-up.sh'
    down_filename = 'vtgate-down.sh'
    short_name = 'vtgate'
    
    def __init__(self, hostname, ls):
        self.hostname = hostname
        self.ls = ls
        self.ports = dict(web_port=15001, grpc_port=15991, mysql_server_port=15306)

    def read_config_interactive(self):
        pass

    def down_commands(self):
        return """#!/bin/bash

# This script stops vtgate.

set -e

pid=`cat $VTDATAROOT/tmp/vtgate.pid`
echo "Stopping vtgate..."
kill $pid
"""
    
    def up_commands(self):
        topology_flags = self.ls.topology_flags
        cell = CELL
        grpc_port = self.ports['grpc_port']
        web_port = self.ports['web_port']
        mysql_server_port = self.ports['mysql_server_port']
        hostname = self.hostname
        
        return """
#!/bin/bash
set -e

# This is an example script that starts a single vtgate.
HOSTNAME="%(hostname)s"
TOPOLOGY_FLAGS="%(topology_flags)s"
CELL="%(cell)s"
GRPC_PORT=%(grpc_port)s
WEB_PORT=%(web_port)s
MYSQL_SERVER_PORT=%(mysql_server_port)s

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

disown -a


""" % locals()

class VtTablet(HostClass):
    up_filename = 'vttablet-up.sh'
    down_filename = 'vttablet-down.sh'
    short_name = 'vttablet'
    
    def __init__(self, hostname, ls, dbconfig, vtctld):
        self.hostname = hostname
        self.ls = ls
        self.dbconfig = dbconfig
        self.vtctld = vtctld

    def read_config_interactive(self):
        # Read number of 'readonly' tablets
        # Read nuber of 'replica' tablets
        # Read the name of shard
        print
        print 'Now we will gather information about vttablets.'
        print
        self.shard = read_value('Enter shard name "0", "80-" "-80" etc.:','0')
        possible_values = ['0', '80-', '-80']
        if self.shard in possible_values:
            self.base_offset = str(100 * (possible_values.index(self.shard) + 1))
        else:
            self.base_offset = read_value('Enter base offset:', '400')
        tablet_types = ['master', 'replica', 'rdonly']
        print
        print "Tablets can be of the follwing types: %s" % tablet_types
        print 'Every shard has one "master" tablet. This starts out as a tablet of type "replica" and is then promoted to "master"'
        print 'We recommend at least one more tablet of type "replica" which is a hot standby for "master" tablet'
        print 'For resharding workflow to work, you need at least 2 tablets of type "rdonly".'
        print 'Thus we recommend a total of 4 tablets, 2 of type "replica" and 2 of type "rdonly".'
        print
        self.num_tablets = read_value('Enter the total number of tablets for this shard:', '4')
        self.num_replicas = read_value('Number of tablets of type "replica":', '2')
        self.num_rdonly = read_value('Number of tablets of type "rdonly":', '2')
        
    def make_header(self):
        topology_flags = self.ls.topology_flags
        cell = CELL
        return """#!/bin/bash
# This script creates a single shard vttablet deployment.

set -e

mkdir -p ${VTDATAROOT}/tmp

CELL=%(cell)s
TOPOLOGY_FLAGS="%(topology_flags)s"
""" % locals()    

    def down_commands(self):
        cell = CELL
        uids = "${@:-'%s'}" % ' '.join([str(i) for i in range(int(self.num_tablets))])
        template="""#!/bin/bash
# This script stops the mysqld and vttablet instances
# created by vttablet-up.sh

CELL="%(cell)s"
BASE_OFFSET=${UID_BASE:-'100'}

# Stop 3 vttablets by default.
# Pass a list of UID indices on the command line to override.
uids=%(uids)s

wait_pids=''

for instance_number in $uids; do
  uid=$[$BASE_OFFSET + $instance_number]
  port=$[$PORT_BASE + $BASE_OFFSET + $instance_number]
  grpc_port=$[$GRPC_PORT_BASE + $BASE_OFFSET + $instance_number]
  printf -v alias '%%s-%%010d' $CELL $uid
  printf -v tablet_dir 'vt_%%010d' $uid

  echo "Stopping vttablet for $alias..."
  pid=`cat $VTDATAROOT/$tablet_dir/vttablet.pid`
  kill $pid
  wait_pids="$wait_pids $pid"

  echo "Stopping MySQL for tablet $alias..."
  $VTROOT/bin/mysqlctl \
    -db-config-dba-uname vt_dba \
    -tablet_uid $uid \
    shutdown &
done

# Wait for vttablets to die.
while ps -p $wait_pids > /dev/null; do sleep 1; done

# Wait for 'mysqlctl shutdown' commands to finish.
wait

"""
        return template % locals()

    def up_commands(self):
        cell = CELL
        dbconfig_dba_flags = self.dbconfig.get_dba_flags()
        dbconfig_flags = self.dbconfig.get_flags()
        init_file = os.path.join(DEPLOYMENT_DIR, 'config', self.dbconfig.init_file)
        vtctld_host = self.vtctld.hostname
        vtctld_web_port = self.vtctld.ports['web_port']
        hostname = self.hostname
        shard = self.shard
        base_offset = self.base_offset
        replica_gate = int(self.num_replicas) - 1
        uids = "${@:-'%s'}" % ' '.join([str(i) for i in range(int(self.num_tablets))])        
        template = r"""
DBCONFIG_DBA_FLAGS=%(dbconfig_dba_flags)s
DBCONFIG_FLAGS=%(dbconfig_flags)s
INIT_DB_SQL_FILE=%(init_file)s
VTCTLD_HOST=%(vtctld_host)s
VTCTLD_WEB_PORT=%(vtctld_web_port)s
HOSTNAME=%(hostname)s

KEYSPACE="${CELL}_keyspace"
SHARD=%(shard)s
BASE_OFFSET=%(base_offset)s
PORT_BASE=15000
GRPC_PORT_BASE=16000
MYSQL_PORT_BASE=17000
replica_gate=%(replica_gate)s

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

mkdir -p $VTDATAROOT/backups

function start_mysql()
{
  instance_number=$1
  uid=$[$BASE_OFFSET + $instance_number]
  mysql_port=$[$MYSQL_PORT_BASE + $BASE_OFFSET + $instance_number]
  printf -v alias '%%s-%%010d' $CELL $uid
  printf -v tablet_dir 'vt_%%010d' $uid

  echo "Starting MySQL for tablet $alias..."
  action="init -init_db_sql_file $INIT_DB_SQL_FILE"
  if [ -d $VTDATAROOT/$tablet_dir ]; then
    echo "Resuming from existing vttablet dir:"
    echo "    $VTDATAROOT/$tablet_dir"
    action='start'
  fi
  $VTROOT/bin/mysqlctl \
    -log_dir $VTDATAROOT/tmp \
    -tablet_uid $uid \
    $DBCONFIG_DBA_FLAGS \
    -mysql_port $mysql_port \
    $action &
}

function start_vttablet() {
  instance_number=$1
  uid=$[$BASE_OFFSET + $instance_number]
  port=$[$PORT_BASE + $BASE_OFFSET + $instance_number]
  grpc_port=$[$GRPC_PORT_BASE + $BASE_OFFSET + $instance_number]
  printf -v alias '%%s-%%010d' $CELL $uid
  printf -v tablet_dir 'vt_%%010d' $uid
  tablet_type=replica
  if [[ $instance_number -gt $replica_gate ]]; then
    tablet_type=rdonly
  fi

  echo "Starting vttablet for $alias..."
  $VTROOT/bin/vttablet \
    $TOPOLOGY_FLAGS \
    -log_dir $VTDATAROOT/tmp \
    -tablet-path $alias \
    -tablet_hostname "$HOSTNAME" \
    -init_keyspace $KEYSPACE \
    -init_shard $SHARD \
    -init_tablet_type $tablet_type \
    -health_check_interval 5s \
    -enable_semi_sync \
    -enable_replication_reporter \
    -backup_storage_implementation file \
    -file_backup_storage_root $VTDATAROOT/backups \
    -restore_from_backup \
    -binlog_use_v3_resharding_mode \
    -port $port \
    -grpc_port $grpc_port \
    -service_map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
    -pid_file $VTDATAROOT/$tablet_dir/vttablet.pid \
    -vtctld_addr http://${VTCTLD_HOST}:${VTCTLD_WEB_PORT}/ \
    $DBCONFIG_FLAGS -v 3 \
    > $VTDATAROOT/$tablet_dir/vttablet.out 2>&1 &

  echo "Access tablet $alias at http://$HOSTNAME:$port/debug/status"
}

# Start 3 vttablets by default.
# Pass a list of UID indices on the command line to override.
uids=%(uids)s

# Start all mysqlds in background.
for instance_number in $uids; do
    start_mysql $instance_number
done

# Wait for all mysqld to start up.
wait

# Start all vttablets in background.
for instance_number in $uids; do
    start_vttablet $instance_number
done

disown -a

"""
        return self.make_header() + template % locals()

def get_public_hostname():
    fqdn = socket.getfqdn()
    # If we are on aws, use the public address.
    if fqdn.endswith('compute.internal'):
        response = urllib2.urlopen('http://169.254.169.254/latest/meta-data/public-hostname')
        return response.read()
    else:
        return fqdn

DEFAULT_PERMISSIONS = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'RELOAD', 'PROCESS', 'FILE',
                       'REFERENCES', 'INDEX', 'ALTER', 'SHOW DATABASES', 'CREATE TEMPORARY TABLES',
                       'LOCK TABLES', 'EXECUTE', 'REPLICATION SLAVE', 'REPLICATION CLIENT', 'CREATE VIEW',
                       'SHOW VIEW', 'CREATE ROUTINE', 'ALTER ROUTINE', 'CREATE USER', 'EVENT', 'TRIGGER']
DB_USERS = {
    'app': {
        'description': 'User for app traffic, with global read-write access.',
        'permissions': DEFAULT_PERMISSIONS,
        },
    'allprivs': {
        'description': 'User for administrative operations that need to be executed as non-SUPER.',
        'permissions': DEFAULT_PERMISSIONS,
        },
    'repl': {
        'description': 'User for slave replication connections.',
        'permissions': ['REPLICATION SLAVE'],
    },
    'filtered': {
        'description': 'User for Vitess filtered replication (binlog player).',
        'permissions': DEFAULT_PERMISSIONS,
        },    
    'dba': {
        'description': 'Admin user with all privilages.',
        'permissions': ['ALL'],
    },    
}

GLOBAL_PARAMS = {
    'charset': 'utf8',
    'dbname': 'vt_%(cell)s_keyspace',

}

class DbConnectionTypes(ConfigType):
    short_name = 'db_config'
    def __init__(self):
        self.dbconfig = {}
        self.vars = {}
        self.db_types = DB_USERS.keys()
        self.init_file = 'init_db.sql'

    def read_config_interactive(self):
        print
        print 'Vitess uses a "Sidecar Database" to store metadata.'
        print 'At the moment, the default name, "_vt" may be hardcoded in vitess at a few places.'
        print 'If you have changed this, enter the new name, otherwise, accept the default.'
        self.sidecar_dbname = read_value('Enter sidecar db name:', '_vt')
        print
        print 'Vitess uses the following connection types to connect to mysql: %s' % DB_USERS.keys()
        print 'Each connection type uses a different user and is used for a different purpose.'
        print 'You can grant each user different privilages.'
        print 'First we will prompt you for usernames, passwords and privilages you want to grant for each of these users.'
        print
        for db_type in DB_USERS:
            self.dbconfig[db_type] = {}
            print '[%s]: %s' % (db_type, DB_USERS[db_type]['description'])
            prompt = 'Enter username for "%s":' % db_type
            default = 'vt_%s' % db_type
            user = read_value(prompt, default)
            self.dbconfig[db_type]['user'] = user        
            prompt = 'Enter password for %s (press Enter for no password):' % user
            password = read_value(prompt)
            self.dbconfig[db_type]['user'] = user
            self.dbconfig[db_type]['password'] = password
            perms = read_value('Enter privlages to be granted to user "%s":' % user, ', '.join(DB_USERS[db_type]['permissions']))
            self.dbconfig[db_type]['permissions'] = perms
            print
        print 'Now we will ask you for parameters used for creating mysql connections that are shared by all connection types'
        self.dbconfig['global'] = {}
        cell = CELL
        for param, default in GLOBAL_PARAMS.iteritems():
            if '%' in param:
                param = param % locals()
            if '%' in default:
                default = default % locals()
            self.dbconfig['global'][param] = read_value('Enter "%s":' % param, default) 

    def write_files(self):
        out = self.make_init_db_sql()
        write_dep_file('config', self.init_file, out)
        
    def get_dba_flags(self):
        charset = self.dbconfig['global']['charset']
        flags = []
        fmt = '-db-config-%(db_type)s-uname %(user)s'
        for db_type in ['dba']:
            user = self.dbconfig[db_type]['user']
            password = self.dbconfig[db_type]['password']
            flags.append(fmt % locals())
            for param, value in self.dbconfig['global'].iteritems():
                if db_type == 'dba' and param == 'dbname':
                    continue                
                flags.append('-db-config-%(db_type)s-%(param)s %(value)s' % locals())
        return '"%s"' % ' '.join(flags)
    
    def get_flags(self):
        keyspace = '%s_keyspace' % CELL
        charset = self.dbconfig['global']['charset']
        flags = []
        fmt = '-db-config-%(db_type)s-uname %(user)s'
        for db_type in DB_USERS:
            user = self.dbconfig[db_type]['user']
            password = self.dbconfig[db_type]['password']
            flags.append(fmt % locals())
            for param, value in self.dbconfig['global'].iteritems():
                if db_type == 'dba' and param == 'dbname':
                    continue
                flags.append('-db-config-%(db_type)s-%(param)s %(value)s' % locals())
            
        return '"%s"' % ' '.join(flags)

    
    def make_init_db_sql(self):
        sidecar_dbname = self.sidecar_dbname
        header = """# This file is executed immediately after mysql_install_db,
# to initialize a fresh data directory.

##########################################
# Equivalent of mysql_secure_installation
##########################################

# Remove anonymous users.
DELETE FROM mysql.user WHERE User = '';

# Disable remote root access (only allow UNIX socket).
DELETE FROM mysql.user WHERE User = 'root' AND Host != 'localhost';

# Remove test database.
DROP DATABASE IF EXISTS test;

##########################################
# Vitess defaults
##########################################

# Vitess-internal database.
CREATE DATABASE IF NOT EXISTS %(sidecar_dbname)s;
# Note that definitions of local_metadata and shard_metadata should be the same
# as in production which is defined in go/vt/mysqlctl/metadata_tables.go.
CREATE TABLE IF NOT EXISTS %(sidecar_dbname)s.local_metadata (
  name VARCHAR(255) NOT NULL,
  value VARCHAR(255) NOT NULL,
  PRIMARY KEY (name)
  ) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS %(sidecar_dbname)s.shard_metadata (
  name VARCHAR(255) NOT NULL,
  value MEDIUMBLOB NOT NULL,
  PRIMARY KEY (name)
  ) ENGINE=InnoDB;

""" % locals()

        footer = """
# User for Orchestrator (https://github.com/github/orchestrator).
GRANT SUPER, PROCESS, REPLICATION SLAVE, RELOAD
  ON *.* TO 'orc_client_user'@'%%' IDENTIFIED BY 'orc_client_user_password';
GRANT SELECT
  ON %(sidecar_dbname)s.* TO 'orc_client_user'@'%%' IDENTIFIED BY 'orc_client_user_password';

FLUSH PRIVILEGES;

""" % locals()
        out = []
        for db_type in self.db_types:
            line = '# %s' % DB_USERS[db_type]['description']
            out.append(line)
            
            perms = self.dbconfig[db_type]['permissions']
            user = self.dbconfig[db_type]['user']
            password = self.dbconfig[db_type]['password']
            hostname = 'localhost'
            if db_type == 'repl':
                hostname = '%'
            line = "GRANT %(perms)s ON *.* TO '%(user)s'@'%(hostname)s'" % locals()
            if password:
                line += " IDENTIFIED BY '%(password)s;" % locals()
            else:
                line += ";"
            out.append(line)
            # If vt_dba add another line.
            if db_type == 'dba':
                line = "GRANT GRANT OPTION ON *.* TO '%(user)s'@'localhost'" % locals()
                if password:
                    line += " IDENTIFIED BY '%(password)s;" % locals()
                else:
                    line += ";"
                    out.append(line)                
            out.append('')

        return header + '\n'.join(out) + footer
        
    def set_vars():
        self.vars['DBCONFIG_DBA_FLAGS'] = None
        self.vars['DBCONFIG_FLAGS'] = None
        self.vars['INIT_DB_SQL_FILE'] = None
        # set DBCONFIG_DBA_FLAGS
        # set DBCONFIG_FLAGS
        # write init_db.sql
        # set INIT_DB_SQL_FILE

def define_args():
    parser = argparse.ArgumentParser(description='Set up host for a vitess deployment role.')
    # We support the following modes:
    # init [local]
    #   Set up lockserver.
    #   Set up vtctld.
    #   Set up vtgate.
    # shard
    # add tablet
    # init cell
    
    prod_parser = parser.add_mutually_exclusive_group(required=False)
    prod_parser.add_argument('--prod', dest='prod', action='store_true', help='apply constraints and give advice for an actual production deployment.')
    prod_parser.add_argument('--no-prod', dest='prod', action='store_false', help='consider this to be a test deployment.')
    parser.set_defaults(prod=False)
    return parser.parse_args()

def create_start_local_cluster(hostname):
    template ="""#!/bin/bash
# This script starts a local cluster.

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo
echo This script will walk you through starting a local vitess cluster.
echo
echo Servers in a Vitess cluster find each other by looking for dynamic configuration data stored in a distributed lock service.
echo After the ZooKeeper cluster is running, we only need to tell each Vitess process how to connect to ZooKeeper.
echo Then, each process can find all of the other Vitess processes by coordinating via ZooKeeper.

echo Each of our scripts automatically uses the TOPOLOGY_FLAGS environment variable to point to the global ZooKeeper instance.
echo The global instance in turn is configured to point to the local instance.
echo In our sample scripts, they are both hosted in the same ZooKeeper service.

echo
echo $DIR/zk-up.sh
read -p "Hit Enter to run the above command ..."
$DIR/zk-up.sh

echo
echo The vtctld server provides a web interface that displays all of the coordination information stored in ZooKeeper.
echo 
echo $DIR/vtctld-up.sh
read -p "Hit Enter to run the above command ..."
$DIR/vtctld-up.sh

echo
echo Open http://%(hostname)s:15000 to verify that vtctld is running. 
echo "There won't be any information there yet, but the menu should come up, which indicates that vtctld is running."

echo The vtctld server also accepts commands from the vtctlclient tool, which is used to administer the cluster.
echo "Note that the port for RPCs (in this case 15999) is different from the web UI port (15000)."
echo These ports can be configured with command-line flags, as demonstrated in vtctld-up.sh.
echo
echo
echo The vttablet-up.sh script brings up three vttablets, and assigns them to a keyspace and shard
echo
echo $DIR/vttablet-up.sh
echo
read -p "Hit Enter to run the above command ..."
$DIR/vttablet-up.sh
echo
echo Next, designate one of the tablets to be the initial master.
echo Vitess will automatically connect the other slaves' mysqld instances so that they start replicating from the master's mysqld.
echo This is also when the default database is created. Since our keyspace is named test_keyspace, the MySQL database will be named vt_test_keyspace.
echo
echo vtctlclient -server %(hostname)s:15999 InitShardMaster -force test_keyspace/0 test-100
read -p "Hit Enter to run the above command ..."
vtctlclient -server %(hostname)s:15999 InitShardMaster -force test_keyspace/0 test-100
echo
echo After running this command, go back to the Shard Status page in the vtctld web interface.
echo When you refresh the page, you should see that one vttablet is the master and the other two are replicas.
echo
echo You can also see this on the command line:
echo
echo vtctlclient -server %(hostname)s:15999 ListAllTablets test
read -p "Hit Enter to run the above command ..."
vtctlclient -server %(hostname)s:15999 ListAllTablets test
echo
echo The vtctlclient tool can be used to apply the database schema across all tablets in a keyspace.
echo The following command creates the table defined in the create_test_table.sql file
echo vtctlclient -server %(hostname)s:15999 ApplySchema -sql "$(cat $VTTOP/examples/local/create_test_table.sql)" test_keyspace
read -p "Hit Enter to run the above command ..."
vtctlclient -server %(hostname)s:15999 ApplySchema -sql "$(cat $VTTOP/examples/local/create_test_table.sql)" test_keyspace
echo
echo "Now that the initial schema is applied, it's a good time to take the first backup. This backup will be used to automatically restore any additional replicas that you run, before they connect themselves to the master and catch up on replication. If an existing tablet goes down and comes back up without its data, it will also automatically restore from the latest backup and then resume replication."
echo vtctlclient -server %(hostname)s:15999 Backup test-0000000102
read -p "Hit Enter to run the above command ..."
vtctlclient -server %(hostname)s:15999 Backup test-0000000102
echo
echo After the backup completes, you can list available backups for the shard:
echo vtctlclient -server %(hostname)s:15999 ListBackups test_keyspace/0
read -p "Hit Enter to run the above command ..."
vtctlclient -server %(hostname)s:15999 ListBackups test_keyspace/0
echo
echo
echo Note: In this single-server example setup, backups are stored at $VTDATAROOT/backups. In a multi-server deployment, you would usually mount an NFS directory there. You can also change the location by setting the -file_backup_storage_root flag on vtctld and vttablet

echo Initialize Vitess Routing Schema
echo "In the examples, we are just using a single database with no specific configuration. So we just need to make that (empty) configuration visible for serving. This is done by running the following command:"
echo vtctlclient -server %(hostname)s:15999 RebuildVSchemaGraph
read -p "Hit Enter to run the above command ..."
vtctlclient -server %(hostname)s:15999 RebuildVSchemaGraph

echo Start vtgate

echo Vitess uses vtgate to route each client query to the correct vttablet. This local example runs a single vtgate instance, though a real deployment would likely run multiple vtgate instances to share the load.

$DIR/vtgate-up.sh

echo Run a simple client application that connects to vtgate and inserst some rows:
for i in `seq 4`; do $VTTOP/examples/local/client.sh; done

echo
echo Congratulations, your local cluster is now up and running.
echo
cat << EOF
You can now explore the cluster:

    Access vtctld web UI at http://%(hostname)s:15000
    Send commands to vtctld with: vtctlclient -server %(hostname)s:15999 ...
    Try "vtctlclient -server %(hostname)s:15999 help".

    Access tablet test-0000000100 at http://%(hostname)s:15100/debug/status
    Access tablet test-0000000101 at http://%(hostname)s:15101/debug/status
    Access tablet test-0000000102 at http://%(hostname)s:15102/debug/status

    Access vtgate at http://%(hostname)s:15001/debug/status
    Connect to vtgate either at grpc_port or mysql_port and run queries against vitess.
EOF

"""
    write_bin_file('start_local_cluster.sh', template % locals())

def create_destroy_local_cluster():
    template = """#!/bin/bash
# This script destroys a local cluster.

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

$DIR/vttablet-down.sh
pkill -f vttablet
pkill -f mysql
$DIR/vtgate-down.sh
pkill -f vtgate
$DIR/vtctld-down.sh
pkill -f vtctld
$DIR/zk-down.sh
pkill -f zksrc
pkill -f zk.cfg
ps -ef | grep zk
ps -ef | grep mysql
ps -ef | grep vt
echo
read -p "Do you want to run: rm -rf $VTDATAROOT [y/N]:"
if [ "$REPLY" == "Y" ] ; then
    rm -rf $VTDATAROOT
    echo Removed $VTDATAROOT
else
    echo Not Rrunning rm -rf $VTDATAROOT
fi

echo

"""
    write_bin_file('destroy_local_cluster.sh', template)

def fix_google_init_file():
    init_file = os.path.join(VTROOT, 'dist/grpc/usr/local/lib/python2.7/site-packages/google/__init__.py')
    if not os.path.exists(init_file) and os.path.exists(os.path.dirname(init_file)):
        subprocess.call(['touch', init_file])

def create_sharding_workflow_script(ls, vtctld):
    topology_flags = ls.topology_flags
    vtctld_host = vtctld.hostname
    vtworker = """#!/bin/bash
# This script runs interactive vtworker.

set -e
echo vtworker.sh $@
read -p "Hit Enter to run the above command ..."
TOPOLOGY_FLAGS="%(topology_flags)s"
exec $VTROOT/bin/vtworker \
  $TOPOLOGY_FLAGS \
  -cell test \
  -log_dir $VTDATAROOT/tmp \
  -alsologtostderr \
  -use_v3_resharding_mode \
  "$@"
"""
    write_bin_file('vtworker.sh', vtworker % locals())
    
    template = r"""#!/bin/bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cat << EOF

The first step is to tell Vitess how we want to partition the data. We do this by providing a VSchema definition as follows:
{
  "sharded": true,
  "vindexes": {
    "hash": {
      "type": "hash"
    }
  },
  "tables": {
    "messages": {
      "column_vindexes": [
        {
          "column": "page",
          "name": "hash"
        }
      ]
    }
  }
}

This says that we want to shard the data by a hash of the page column. In other words, keep each page's messages together, but spread pages around the shards randomly.

We can load this VSchema into Vitess like this:
EOF

echo vtctlclient -server %(vtctld_host)s:15999 ApplyVSchema -vschema "$(cat $VTTOP/examples/local/vschema.json)" test_keyspace
read -p "Hit Enter to run the above command ..."
vtctlclient -server %(vtctld_host)s:15999 ApplyVSchema -vschema "$(cat $VTTOP/examples/local/vschema.json)" test_keyspace

cat << EOF

Now bring up tablets on 2 other hosts for shards -80 and 80- using deployment_helper.py
(When prompted for vtctld, please point to %(vtctld_host)s)

EOF

read -p "Hit Enter when you have finished starting the tablets for shards -80 and 80- ..."

cat << EOF

Now, run the following command.  You should be able to see tablets for all 3 shards.

EOF

echo vtctlclient -server %(vtctld_host)s:15999 ListAllTablets
read -p "Hit Enter to run the above command ..."

vtctlclient -server %(vtctld_host)s:15999 ListAllTablets test

cat << EOF
Once the tablets are ready, initialize replication by electing the first master for each of the new shards:
EOF
for shard in "80-" "-80"; do 
    tablet=$(vtctlclient -server %(vtctld_host):15999 ListShardTablets test_keyspace/$shard | head -1 | awk '{print $1}')
    echo vtctlclient -server %(vtctld_host)s:15999 InitShardMaster -force test_keyspace/$shard $tablet
    read -p "Hit Enter to run the above command ..."
    vtctlclient -server %(vtctld_host)s:15999 InitShardMaster -force test_keyspace/$shard $tablet
done

cat << EOF
Now there should be a total of 15 tablets, with one master for each shard:
EOF

echo vtctlclient -server %(vtctld_host)s:15999 ListAllTablets test
read -p "Hit Enter to run the above command ..."
vtctlclient -server %(vtctld_host)s:15999 ListAllTablets test
cat << EOF
The new tablets start out empty, so we need to copy everything from the original shard to the two new ones.

We first copy schema:
EOF

echo vtctlclient -server %(vtctld_host)s:15999 CopySchemaShard test_keyspace/0 test_keyspace/-80
read -p "Hit Enter to run the above command ..."
vtctlclient -server %(vtctld_host)s:15999 CopySchemaShard test_keyspace/0 test_keyspace/-80
echo vtctlclient -server %(vtctld_host)s:15999 CopySchemaShard test_keyspace/0 test_keyspace/80-
read -p "Hit Enter to run the above command ..."
vtctlclient -server %(vtctld_host)s:15999 CopySchemaShard test_keyspace/0 test_keyspace/80-

cat << EOF

Next we copy the data. Since the amount of data to copy can be very large, we use a special batch process
called vtworker to stream the data from a single source to multiple destinations, routing each row based on its keyspace_id.

Notice that we only needed to specifiy the source shard, test_keyspace/0. 
The SplitClone process will automatically figure out which shards to use as the destinations based on the key range that needs to be covered.
In this case, shard 0 covers the entire range, so it identifies -80 and 80- as the destination shards, since they combine to cover the same range.

Next, it will pause replication on one rdonly (offline processing) tablet to serve as a consistent snapshot of the data. 
The app can continue without downtime, since live traffic is served by replica and master tablets, which are unaffected. 
Other batch jobs will also be unaffected, since they will be served only by the remaining, un-paused rdonly tablets.

Once the copy from the paused snapshot finishes, vtworker turns on filtered replication from the source shard to each destination shard. 
This allows the destination shards to catch up on updates that have continued to flow in from the app since the time of the snapshot.

EOF

$DIR/vtworker.sh SplitClone test_keyspace/0

cat << EOF

When the destination shards are caught up, they will continue to replicate new updates.
You can see this by looking at the contents of each shard as you add new messages to various pages in the Guestbook app.
Shard 0 will see all the messages, while the new shards will only see messages for pages that live on that shard.

Let us add a few rows to shard 0.

EOF

read -p "Hit Enter to add 6 rows to the database ..."
for i in `seq 2`; do $VTTOP/examples/local/client.sh; done

echo See data on shard test_keyspace/0:
echo vtctlclient -server %(vtctld_host)s:15999 ExecuteFetchAsDba test-0000000100 "SELECT count(*) FROM messages"
read -p "Hit Enter to run the above command ..."
vtctlclient -server %(vtctld_host)s:15999 ExecuteFetchAsDba test-0000000100 "SELECT count(*) FROM messages"

echo See data on shard test_keyspace/-80:
echo vtctlclient -server %(vtctld_host)s:15999 ExecuteFetchAsDba test-0000000200 "SELECT count(*) FROM messages"
read -p "Hit Enter to run the above command ..."
vtctlclient -server %(vtctld_host)s:15999 ExecuteFetchAsDba test-0000000200 "SELECT count(*) FROM messages"

echo See data on shard test_keyspace/80-:
echo vtctlclient -server %(vtctld_host)s:15999 ExecuteFetchAsDba test-0000000300 "SELECT count(*) FROM messages"
read -p "Hit Enter to run the above command ..."
vtctlclient -server %(vtctld_host)s:15999 ExecuteFetchAsDba test-0000000300 "SELECT count(*) FROM messages"

cat << EOF

Now let us check copied data integrity

The vtworker batch process has another mode that will compare the source and destination
to ensure all the data is present and correct.

EOF

echo for -80
$DIR/vtworker.sh SplitDiff test_keyspace/-80
echo for 80-
$DIR/vtworker.sh SplitDiff test_keyspace/80-

cat << EOF

Now we are ready to switch over to serving from the new shards. 
The MigrateServedTypes command lets you do this one tablet type at a time, and even one cell at a time. 
The process can be rolled back at any point until the master is switched over.
EOF

echo vtctlclient -server %(vtctld_host)s:15999 MigrateServedTypes test_keyspace/0 rdonly
read -p "Hit Enter to run the above command ..."
vtctlclient -server %(vtctld_host)s:15999 MigrateServedTypes test_keyspace/0 rdonly

echo vtctlclient -server %(vtctld_host)s:15999 MigrateServedTypes test_keyspace/0 replica
read -p "Hit Enter to run the above command ..."
vtctlclient -server %(vtctld_host)s:15999 MigrateServedTypes test_keyspace/0 replica

echo vtctlclient -server %(vtctld_host)s:15999 MigrateServedTypes test_keyspace/0 master
read -p "Hit Enter to run the above command ..."
vtctlclient -server %(vtctld_host)s:15999 MigrateServedTypes test_keyspace/0 master

cat << EOF

During the master migration, the original shard master will first stop accepting updates. 
Then the process will wait for the new shard masters to fully catch up on filtered replication before allowing them to begin serving. 
Since filtered replication has been following along with live updates, there should only be a few seconds of master unavailability.

When the master traffic is migrated, the filtered replication will be stopped. 
Data updates will be visible on the new shards, but not on the original shard. 
See it for yourself: Let us add a few rows and then inspect the database content.

EOF

read -p "Hit Enter to add 6 rows ..."
for i in `seq 2`; do $VTTOP/examples/local/client.sh; done

echo See data on shard test_keyspace/0
echo "(no updates visible since we migrated away from it):"
echo vtctlclient -server %(vtctld_host)s:15999 ExecuteFetchAsDba test-0000000100 "SELECT count(*) FROM messages"
read -p "Hit Enter to run the above command ..."
vtctlclient -server %(vtctld_host)s:15999 ExecuteFetchAsDba test-0000000100 "SELECT count(*) FROM messages"
echo
echo "See data on shard test_keyspace/-80:"
echo vtctlclient -server %(vtctld_host)s:15999 ExecuteFetchAsDba test-0000000200 "SELECT count(*) FROM messages"
vtctlclient -server %(vtctld_host)s:15999 ExecuteFetchAsDba test-0000000200 "SELECT count(*) FROM messages"
echo
echo "See data on shard test_keyspace/80-:"
echo vtctlclient -server %(vtctld_host)s:15999 ExecuteFetchAsDba test-0000000300 "SELECT count(*) FROM messages"
read -p "Hit Enter to run the above command ..."
vtctlclient -server %(vtctld_host)s:15999 ExecuteFetchAsDba test-0000000300 "SELECT count(*) FROM messages"

cat << EOF

Now that all traffic is being served from the new shards, we can remove the original one.
Fist we shut down the vttablets for the unused shard (shard "0").

EOF

echo $DIR/vttablet-down.sh
read -p "Hit Enter to run the above command ..."
$DIR/vttablet-down.sh

cat << EOF

Then we can delete the now-empty shard:

EOF

echo vtctlclient -server %(vtctld_host)s:15999 DeleteShard -recursive test_keyspace/0
vtctlclient -server %(vtctld_host)s:15999 DeleteShard -recursive test_keyspace/0
read -p "Hit Enter to run the above command ..."

echo
echo Congratulations, you have succesfully resharded your database.
echo Look at http://%(vtctld_host)s:15000/ and verify that you only see shards 80- and -80.
echo 
"""
    write_bin_file("run_sharding_workflow.sh", template % locals())
    
def main():
    args = define_args()
    public_hostname = get_public_hostname()
    print 'This host is:'
    print '\tlocalhost = %s' % g_local_hostname
    if g_local_hostname != public_hostname:
        print '\tpublic_hostname = %s' % public_hostname
        print
    check_host()

    # fix google init file so that python programs can find protobufs
    fix_google_init_file()
    public_hostname = get_public_hostname()
    local_hostname = g_local_hostname    

    is_vtctld = read_value('Do you have a vtctld process running in your deploment? [Y/N] :', 'N')
    if is_vtctld == 'Y':
        vtctld_host = read_value('Enter the vtctld hostname: ')
        vtctld_port = read_value('Enter vtctld port number: ', str(base_ports['vtctld']['grpc_port']))
        vtctld_endpoint = '%s:%s' % (vtctld_host, vtctld_port)
        print 'Connecting to vtctld to get topological information at "%s".' % vtctld_endpoint
        cmd = ['vtctlclient', '-server', vtctld_endpoint, 'GetCellInfoNames']
        cells = subprocess.check_output(cmd).split('\n')
        print 'Found cells: %s' % cells
        set_cell(cells[0])
        cmd = ['vtctlclient', '-server', vtctld_endpoint, 'GetCellInfo', CELL]
        print ' '.join(cmd)
        cell_info = json.loads(subprocess.check_output(cmd))
        print cell_info
        ls = LockServer()
        ls.set_topology_from_vtctld(cell_info)
        vtctld = VtCtld(vtctld_host, ls)
        dbcfg = DbConnectionTypes()
        dbcfg.read_config()
        dbcfg.write_files()
        vttablet = VtTablet(public_hostname, ls, dbcfg, vtctld)
        vttablet.read_config()
        vttablet.write_files()
        print
        print 'The following scripts were generated under: %s' % DEPLOYMENT_DIR
        for c in [vttablet]:
            print '\t%s' % c.up_filename
            print '\t%s' % c.down_filename
        start_local = os.path.join(DEPLOYMENT_DIR, 'bin', vttablet.up_filename)
        response = read_value('Run "%s" to start vttablet now? :' % start_local, 'Y')
        if response == 'Y':
            subprocess.call(['bash', start_local])
    else:
        set_cell('test')
        ls = LockServer()
        ls.read_config()
        ls.write_files()

        vtctld = VtCtld(public_hostname, ls)
        #vtctld = VtCtld(local_hostname, ls)
        vtctld.read_config()
        vtctld.write_files()

        vtgate = VtGate(public_hostname, ls)
        #vtgate = VtGate(local_hostname, ls)        
        vtgate.read_config()
        vtgate.write_files()

        dbcfg = DbConnectionTypes()
        dbcfg.read_config()
        dbcfg.write_files()
        vttablet = VtTablet(public_hostname, ls, dbcfg, vtctld)
        vttablet.read_config()
        vttablet.write_files()

        create_start_local_cluster(public_hostname)
        create_destroy_local_cluster()
        create_sharding_workflow_script(ls, vtctld)
        
        print
        print 'The following scripts were generated under: %s' % DEPLOYMENT_DIR
        for c in [ls.ls, vtctld, vttablet, vtgate]:
            print '\t%s' % c.up_filename
            print '\t%s' % c.down_filename
        print '\t%s' % 'start_local_cluster.sh'
        print '\t%s' % 'destroy_local_cluster.sh'
        print '\t%s' % 'run_sharding_workflow.sh'        
        print
        # print 'Note: Vitess binaries create log files under: %s' % os.path.join(VTDATAROOT, 'tmp')

        start_local = os.path.join(DEPLOYMENT_DIR, 'bin', 'start_local_cluster.sh')
        response = read_value('Run "%s" to start local cluster now? :' % start_local, 'Y')
        if response == 'Y':
            subprocess.call(['bash', start_local])

        print
        
        run_sharding = os.path.join(DEPLOYMENT_DIR, 'bin', 'run_sharding_workflow.sh')
        response = read_value('Run "%s" to demo sharding workflow now? :' % start_local, 'Y')
        if response == 'Y':
            subprocess.call(['bash', run_sharding])                    

if __name__ == '__main__':
    main()
    
