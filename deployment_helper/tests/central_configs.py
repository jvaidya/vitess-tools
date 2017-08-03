import json
import os
import subprocess
import tempfile

class ConfigInZk(object):
    ZK_DEPLOYMENT_HELPER_ROOT = '/zk/vitess/tools/deployment_helper'

    def __init__(self, zk_server):
        self.zk_server = zk_server
        self.zk_cmd = ['zk', '-server', zk_server]
        self.set_config_paths()
        
    def set_config_paths(self):
        # We might want to add "cell", "shard", possibly "host" hierarchy here.
        self.config_root = os.path.join(self.ZK_DEPLOYMENT_HELPER_ROOT, 'config')
    
    def fetch(self, name):
        config_path = os.path.join(self.config_root, name)
        cmd = self.zk_cmd + ['cat', config_path]
        return json.loads(subprocess.check_output(cmd))

    def save(self, name, config):
        config_path = os.path.join(self.config_root, name)
        _, local_path = tempfile.mkstemp()
        with open(local_path, 'w') as fh:
            json.dump(config, fh, indent=4, separators=(',', ': '))        
        cmd = self.zk_cmd + ['touch', '-p', self.config_root]
        subprocess.check_call(cmd)
        cmd = self.zk_cmd + ['cp', local_path, config_path]
        subprocess.check_call(cmd)    
        os.unlink(local_path)

def test():
    config = dict(a=1, b=2, c=3)
    zk_server = 'localhost:21811'
    cfg = ConfigInZk(zk_server)
    cfg.save('dbconfig', config)
    read_config = cfg.fetch('dbconfig')
    assert config == read_config

if __name__ == '__main__':
    test()


