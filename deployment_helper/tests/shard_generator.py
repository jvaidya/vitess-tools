import sys

NUM_BYTES = 1
MAX_SHARDS = 2 ** (NUM_BYTES * 8)

def make_shards(num_shards):
    def get_str(x):
        if x in (hex(0), hex(MAX_SHARDS)):
            return ''
        rv = '%s' % x
        return rv.replace('0x', '')

    start = hex(0)
    end = None
    size = MAX_SHARDS / num_shards
    shards = []
    for i in xrange(1, num_shards + 1):
        end = hex(i * size)
        shards.append('%s-%s' % (get_str(start), get_str(end)))
        start = end
    return shards

print 'MAX_SHARDS = %d' % MAX_SHARDS

if len(sys.argv) > 1:
    num_shards = int(sys.argv[1])
    print make_shards(num_shards)
else:
    for i in xrange(1, (NUM_BYTES * 8) + 1):
        num_shards = 2 ** i
        print 'num_shards = %s' % num_shards
        print make_shards(num_shards)
    
