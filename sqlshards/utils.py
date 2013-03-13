"""
   Copyright 2013 DISQUS
   
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
   
       http://www.apache.org/licenses/LICENSE-2.0
   
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

class DatabaseConfigurator(object):
    """
    Similar to the logging DictConfigurator, DatabaseConfigurator allows simply
    inheritance within a dictionary.

    It also looks for a special key (Django is not aware of it) called SHARDS
    which contains some logic for automatically creating additional connections
    based on the named connection.

    Additionally it handles a field called HOSTS, which is only used in conjuction
    with SHARDS. If this is set, it will handle mapping the underlying shards
    to other physical machines so that a shard's host is hosts[<shard number> % <num hosts>].
    """
    def __init__(self, settings, defaults={}):
        self.settings = settings
        self.defaults = defaults

    def __iter__(self):
        for alias in self.settings.iterkeys():
            config = self.get_value(alias)

            shard_info = config.get('SHARDS', {})
            host_list = config.get('HOSTS')

            if not shard_info and not host_list:
                yield alias, config
                continue

            # if servers are present this is a virtual connection mapping to N connections
            hosts = {}
            if host_list:
                for num, host_config in host_list.iteritems():
                    for key, value in config.iteritems():
                        if key in ('SHARDS', 'HOSTS'):
                            continue
                        host_config.setdefault(key, value)
                    hosts[num] = host_config
            else:
                hosts[0] = config

            assert not set(hosts.iterkeys()).difference(set(xrange(len(hosts)))), 'Host indexes must not contain any gaps'

            cluster_size = shard_info.get('size') or len(hosts)
            assert cluster_size > 0, 'Cluster cannot be empty'

            # we create ``size`` new connections based on the parent connections
            for num in xrange(cluster_size):
                shard = '%s.shard%d' % (alias, num)
                host_num = num % len(hosts)

                shard_n_config = hosts[host_num].copy()

                # test mirror can vary if its referencing a clustered connection
                if not shard_n_config.get('TEST_MIRROR'):
                    shard_n_config['TEST_MIRROR'] = alias

                # ensure test mirror points to parent shard if a mirror is not
                # explicitly set
                yield shard, shard_n_config

            # maintain the original connection for dev/test environments
            yield alias, config

    def get_parent_value(self, key):
        defaults = self.defaults.copy()

        if '.' in key:
            parts = key.split('.')
            for subkey in ('.'.join(parts[:(idx + 1)]) for idx in xrange(len(parts) - 1)):
                if subkey in self.settings:
                    defaults.update(self.settings[subkey])

        return defaults

    def get_value(self, key):
        defaults = self.get_parent_value(key)
        defaults.update(self.settings[key])

        return defaults

    def to_dict(self):
        return dict(self)


def wraps(func):
    """
    Nearly identical to functools.wraps, except that it also
    maintains the ``__wraps__`` attribute, which will always
    be the origin function.
    """
    def wrapped(wrapper):
        actual = getattr(func, '__wraps__', func)
        for attr in ('__module__', '__name__', '__doc__'):
            if hasattr(actual, attr):
                setattr(wrapper, attr, getattr(actual, attr))
        wrapper.__wraps__ = actual
        return wrapper
    return wrapped
