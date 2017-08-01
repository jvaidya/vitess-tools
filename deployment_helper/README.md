# deployment_helper

This version:

+ Gathers input from you about the cluster (there are defaults for all prompts, hit Enter to accept them)
+ Creates the scripts for starting the cluster
+ Runs these scripts to start the local cluster, initialize a master tablet and then demonstrates insertion and reading of rows from it.
+ Then it starts "run_resharding_workflow.sh" that starts the resharding workflow.

The "running of the demo" functionality is independent of the core deployment_helper functionality and will be factored out into separate scripts soon.

