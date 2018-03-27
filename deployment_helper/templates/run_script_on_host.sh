#!/bin/bash

SSH_OPTS="-q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
AWS_PUBLIC_HOSTNAME_URL="http://169.254.169.254/latest/meta-data/public-hostname"

function check_ssh()
{
    host=$1
    ssh $SSH_OPTS $host -- hostname > /dev/null
    echo $?
}

function is_local()
{
    cmd_host=$1
    host=$(hostname -f)
    if [ "$cmd_host" == "$host" ]; then
	echo 1
	return
    fi
    host=$(hostname)
    if [ "$cmd_host" == "$host" ]; then
	echo 1
	return
    fi
    # OSX does not support hostname -i
    if [ "$(uname)" != Darwin -a "$cmd_host" == "$(hostname -i)" ]; then
	echo 1
	return
    fi
    host=$(curl -s -m 1 $AWS_PUBLIC_HOSTNAME_URL)
    if [ "$cmd_host" == "$host" ]; then
	echo 1
	return
    fi
    echo 0
}

function log()
{
    if [ $VERBOSE -eq 1 ]; then
	echo $@
    fi
}

function run_script_file()
{
    host=$1
    script_file=$2
    config_file=$3

    local=$(is_local $host)
    if [ $local -eq 1 ]; then
	echo Running $script_file locally
	$script_file
    else
	can_not_ssh=$(check_ssh $host)
	if [ $can_not_ssh -ne 0 ]; then
	    echo Unable to ssh to $host
	    echo Failed to run $script_file on $host
	    exit $can_not_ssh
	fi
	echo Running $script_file remotely on $host
	cmd_dir=$(dirname $script_file)
	log Making $cmd_dir on $host
	ssh $SSH_OPTS $host -- mkdir -p $cmd_dir
	log Copying $script_file to $host
	scp $SSH_OPTS $script_file $host:$script_file
	if [ "$config_file" != "" ]; then
	    config_dir=$(dirname $config_file)
	    log Making $config_dir on $host
	    ssh $SSH_OPTS $host -- mkdir -p $config_dir
	    log Copying $config_file to $host
	    scp $SSH_OPTS $config_file $host:$config_file
	fi
	log Starting $script_file on $host
	ssh $SSH_OPTS $host -- $script_file
    fi
}

VERBOSE=0

run_script_file $1 $2 $3
