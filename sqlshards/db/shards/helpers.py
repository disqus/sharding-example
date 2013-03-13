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

def get_sharded_id_sequence_name(model):
    # XXX: This matches what PostgreSQL would normally generate for a serial
    # type.  This is needed because the old AutoSequenceField sets up some
    # signals which assume the sequence name that @replace_pk overwrites.
    return '%s_%s_seq' % (model._meta.db_table, model._meta.pk.column)


def get_canonical_model(model):
    """
    Accepts a model class, returning the canonical parent model if the model is
    a child of an abstract partitioned model.
    """
    if is_partitioned(model) and model._shards.is_child:
        model = model._shards.parent

    return model


#: Returns ``True`` if the given class is a partitioned model.
is_partitioned = lambda cls: hasattr(cls, '_shards')
