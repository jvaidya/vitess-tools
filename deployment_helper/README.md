# deployment_helper

I wrote deployment_helper with three (progressively sophisticated)
audiences in mind:

1. Somebody new to Vitess who has just downloaded and compiled vitess and now wants to take it for a spin. For this audience deployment_helper replaces the scripts under examples/local.

2. Somebody who has allocated, say, 20 hosts for a vitess deployment and wants to see how a 16 shard or 64 shad deployment would look like.

3. Somebody who already has a vitess deployment and wants help managing the cluster. Wants a central repository for commandline templates, central repository for database grants and user authentication information and an easy way to allocate, start and stop tablets.

The current version is definitely an alpha. It works well as long as
you give it correct inputs. And although it has been written with all 3 audiences in mind, the current defaults very much lean towards the new user. So the tool, with the current defaults, does a lot of explaining and asking as it gathers inputs from you. Also, some information that tool shows you are placeholders, you will see some "x cpus and y memory" in there.

With these caveats, here is a video in which I demonstrate  bringing up a Vitess cluster on 4 aws hosts, starting with 2 shards and then resharding to 4 shards :

https://www.youtube.com/watch?v=oH3tzHl68Bg

Try running:
```python deployment_helper.py --action run_demo```
This will:

+ Gather input from you about the cluster (there are defaults for all prompts, hit Enter to accept them)
+ Generate the scripts for starting the cluster
+ Run these scripts to start the cluster, initialize master tablets and then demonstrates insertion and reading of rows from it.
+ Prompt you to start "run_resharding_workflow.sh" that starts the resharding workflow.
