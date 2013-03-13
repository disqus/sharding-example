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

class ShardedRouter(object):
    """
    Breaks up apps based on their attached shard info.

    This looks for "_shards" on the model (which is defined as part of PartitionBase)
    and ensures only child tables get synced, as well as guarantees the correct (master)
    database for queries on a given shard.
    """
    def db_for_read(self, model, **hints):
        shard_info = getattr(model, '_shards', None)
        if shard_info:
            if not shard_info.is_child:
                raise ValueError('%r cannot be queried as its a virtual partition model' % model.__name__)
            return shard_info.get_database()

        return None

    def db_for_write(self, model, **hints):
        hints['is_write'] = True
        return self.db_for_read(model, **hints)

    def allow_syncdb(self, db, model):
        shard_info = getattr(model, '_shards', None)
        if shard_info:
            if db == shard_info.cluster:
                return True
            if shard_info.is_child and db in shard_info.get_all_databases():
                return True
            return False

        return None
