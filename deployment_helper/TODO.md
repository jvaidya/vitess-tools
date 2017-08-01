

# TODO list for deployment helper

### General
  * In the sharding demo, add the step to start replication back to the old master.
  * Support generation of commands for non-local hosts.
  * Distinguish between PUBLIC_HOSTNAME and HOSTNAME
  * Remote command execution.
  * Ability to ship config around? Write config to zk?
  * Support for mysql and vttablet running on different hosts

### Implement Tablet distribution logic
    Input:
        set of hosts
        set of shards
        per shard tablet config
        [optional: current configuration]
    Output: per host tablet start and stop commands.

### Three host demo
	distributed zk
	distributed tablets for shards
	
### Commandline design and implementation
	deployment_help action component [modifiers]
	actions: configure, generate, start, stop, run_demo
	components: lockserver, vtctld, vttablets, vtgate, mysql, all
	modifiers: --interactive, --vtctld-addr, --verbose

### Possible states and code for reconciliation

	Config state
	Process state (are processes running as per config)
	Canonical state (state as known by vtctld)
