import random
import pprint

pp = pprint.PrettyPrinter(indent=4)

def evaluate_host(shard, jobs):
    count = len(jobs)
    shard_count = 0
    for j in jobs:
        if j.startswith(shard):
            shard_count += 1
            if j.startswith('%s_master' % shard):
                shard_count += 1
    return count + shard_count

def distribute_tablets(shards, num_instances, configured_hosts):
    jobs_per_host = {}
    for tablet_type in [ 'master', 'replica', 'rdonly']:
        jobs = []
        for shard in shards:
            jobs += ['%s_%s_%d' % (shard, tablet_type, i) for i in xrange(1, num_instances[tablet_type] + 1)]
        for job in jobs:
            shard, tablet_type, _ = job.split('_')
            used_hosts = set(jobs_per_host.keys())
            unused_hosts = set(configured_hosts) - used_hosts
            if unused_hosts:
                host = random.choice(list(unused_hosts))
            else:
                candidates = configured_hosts
                candidates.sort(key=lambda h: evaluate_host(shard, jobs_per_host[h]))
                host = candidates[0]
            if host not in jobs_per_host:
                jobs_per_host[host] = []
            jobs_per_host[host].append(job)
    return jobs_per_host

def get_next_host(hosts, configured_hosts):
    usable_hosts = set(configured_hosts)
    used_hosts = set(hosts)
    unused_hosts = usable_hosts - used_hosts
    if unused_hosts:
        return random.choice(list(unused_hosts))
    usage = {str(hosts.count(h)) : h for h in used_hosts}
    min_used_num = sorted([int(i) for i in usage.keys()])[0]
    return usage[str(min_used_num)]

def distribute_jobs(jobs, configured_hosts):
    hosts = []
    jobs_per_host = {}
    for i, job in enumerate(jobs):
        host = get_next_host(hosts, configured_hosts)
        hosts.append(host)
        if host not in jobs_per_host:
            jobs_per_host[host] = []
        jobs_per_host[host].append(job)
    return jobs_per_host

def test(num_jobs, num_hosts):
    configured_hosts = [ 'host_%d' % i for i in xrange(1, num_hosts + 1) ]
    jobs = [ 'job_%d' % i for i in xrange(1, num_jobs + 1) ]
    jobs_per_host = distribute_jobs(jobs, configured_hosts)
    print 'num_jobs = %d num_hosts = %d' % (num_jobs, num_hosts)
    pp.pprint(jobs_per_host)
    print

def check_master(dist):
    for h in dist:
        master_job = [j for j in dist[h] if 'master' in j][0]
        shard, rtype, i = master_job.split('_')
        shard_jobs = [j for j in dist[h] if shard in j]
        assert len(shard_jobs) == 1
    print 'Success: master tablets do not share hosts with replica or rdonly'

def test_ds(shards, num_hosts):
    num_instances = dict(master=1, replica=2, rdonly=2)
    configured_hosts = [ 'host_%d' % i for i in xrange(1, num_hosts + 1) ]
    print 'num_hosts = %d, shards = %s, %s' % (num_hosts, shards, num_instances)
    dist = distribute_tablets(shards, num_instances, configured_hosts)
    print_dist(dist)
    if len(shards) == num_hosts:
        # make sure that we do not share master with other replicas
        check_master(dist)
    print

def print_dist(dist):
    print
    for h in dist:
        print '"%s" = %d' % (h, len(dist[h]))
    pp.pprint(dist)

if __name__ == '__main__':
    test(5, 1)
    test(1, 5)
    test(5, 5)
    test(20, 5)

    test_ds(["shard-1", "shard-2", "shard-3"], 3)
    test_ds(["shard-1", "shard-2", "shard-3"], 2)
    test_ds(["shard-1", "shard-2", "shard-3"], 5)
    test_ds(["shard-1", "shard-2", "shard-3", "shard-4","shard-5"], 5)
