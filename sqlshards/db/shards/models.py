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

import sys

from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned, ValidationError
from django.db import connections, transaction
from django.db.models import loading, Manager, Model, signals
from django.db.models.base import ModelBase, subclass_exception
from django.db.models.fields import PositiveIntegerField
from django.db.models.fields.related import ForeignKey, ManyToOneRel, \
  RECURSIVE_RELATIONSHIP_CONSTANT, ReverseSingleRelatedObjectDescriptor
from django.db.utils import DatabaseError

from sqlshards.db.shards.fields import AutoSequenceField
from sqlshards.db.shards.helpers import get_sharded_id_sequence_name
from sqlshards.db.shards.manager import MasterPartitionManager
from sqlshards.utils import wraps


class PartitionedForeignKey(ForeignKey):
    """
    Behaves identical to a ForeignKey except it allows referencing an fkey
    that would live on the same cluster.
    """
    def __init__(self, to, to_field=None, rel_class=ManyToOneRel, **kwargs):
        try:
            to._meta.object_name.lower()
        except AttributeError:  # to._meta doesn't exist, so it must be RECURSIVE_RELATIONSHIP_CONSTANT
            assert isinstance(to, basestring), "%s(%r) is invalid. First parameter to ForeignKey must be either a model, a model name, or the string %r" % (self.__class__.__name__, to, RECURSIVE_RELATIONSHIP_CONSTANT)
        else:
            # For backwards compatibility purposes, we need to *try* and set
            # the to_field during FK construction. It won't be guaranteed to
            # be correct until contribute_to_class is called. Refs #12190.
            to_field = to_field or (to._meta.pk and to._meta.pk.name)
        kwargs['verbose_name'] = kwargs.get('verbose_name', None)

        kwargs['rel'] = rel_class(to, to_field,
            related_name=kwargs.pop('related_name', None),
            limit_choices_to=kwargs.pop('limit_choices_to', None),
            parent_link=kwargs.pop('parent_link', False))
        super(ForeignKey, self).__init__(**kwargs)

        self.db_index = True

    def contribute_to_related_class(self, cls, related):
        # Reverse lookups not supported currently
        return

    def contribute_to_class(self, cls, name):
        # Pull in PartitionedReverseRelatedObjectDescriptor
        super(ForeignKey, self).contribute_to_class(cls, name)
        setattr(cls, self.name, PartitionedReverseRelatedObjectDescriptor(self))
        if isinstance(self.rel.to, basestring):
            target = self.rel.to
        else:
            target = self.rel.to._meta.db_table
        cls._meta.duplicate_targets[self.column] = (target, "o2m")

    def south_field_triple(self):
        "Returns a suitable description of this field for South."
        from south.modelsinspector import introspector
        field_class = "django.db.models.PositiveIntegerField"
        args, kwargs = introspector(self)
        return (field_class, [], {'db_column': "%r" % self.column})


class PartitionedReverseRelatedObjectDescriptor(ReverseSingleRelatedObjectDescriptor):
    """
    Identical to ReverseSingleRelatedObjectDescriptor except that we pass the key
    to the manager and relying on outside routing.
    """
    def __get__(self, instance, instance_type=None):
        if instance is None:
            return self

        cache_name = self.field.get_cache_name()
        try:
            return getattr(instance, cache_name)
        except AttributeError:
            val = getattr(instance, self.field.attname)
            if val is None:
                # If NULL is an allowed value, return it.
                if self.field.null:
                    return None
                raise self.field.rel.to.DoesNotExist
            other_field = self.field.rel.get_related_field()
            relname = self.field.rel.field_name
            if other_field.rel:
                params = {'%s__pk' % relname: val}
            else:
                params = {'%s__exact' % relname: val}

            # Ensure key is sent to the manager
            for field_name in instance._shards.key:
                params[field_name] = getattr(instance, field_name)

            # If the related manager indicates that it should be used for
            # related fields, respect that.
            rel_mgr = self.field.rel.to._default_manager
            rel_obj = rel_mgr.get(**params)
            setattr(instance, cache_name, rel_obj)
            return rel_obj


def resend_signal(new_sender):
    @wraps(new_sender)
    def wrapped(**kwargs):
        signal = kwargs.pop('signal')
        kwargs['sender'] = new_sender
        signal.send(**kwargs)
    return wrapped


def get_cluster_sizes(connections):
    """
    Returns a dictionary mapping clusters of servers (given
    by their naming scheme) and the number of connections in
    that cluster.
    """
    import re
    expr = re.compile(r'.*\.shard\d+$')
    clusters = {}
    for conn in connections:
        if not expr.match(conn):
            continue
        cluster = conn.split('.shard', 1)[0]
        if cluster not in clusters:
            clusters[cluster] = 1
        else:
            clusters[cluster] += 1
    return clusters


DEFAULT_NAMES = ('num_shards', 'key', 'sequence', 'abstract', 'cluster')
CLUSTER_SIZES = get_cluster_sizes(connections)


class MasterShardOptions(object):
    def __init__(self, options, nodes=[]):
        self.options = options
        self.nodes = nodes
        self.model = None
        self.name = None
        self.size = None

    def __repr__(self):
        return u'<%s: model=%s, options=%s, nodes=%s>' % (
            self.__class__.__name__, self.model,
            self.options, len(self.nodes))

    @property
    def is_child(self):
        return False

    @property
    def is_master(self):
        return True

    def contribute_to_class(self, cls, name):
        self.name = name
        self.model = cls
        setattr(cls, name, self)

        opts = self.options

        if opts:
            for k in (k for k in DEFAULT_NAMES if hasattr(opts, k)):
                setattr(self, k, getattr(opts, k))

        if not hasattr(self, 'sequence'):
            self.sequence = cls._meta.db_table

        if hasattr(self, 'cluster'):
            self.size = CLUSTER_SIZES[self.cluster]

        # We support both key = 'field_name' and key = ['field_name']
        # style declerations
        if hasattr(self, 'key') and isinstance(self.key, basestring):
            self.key = (self.key,)

    def get_key_from_instance(self, instance):
        """
        Return the routing key for an instance.

        >>> shard_key = Model._shards.get_key_from_instance(instance)
        """
        return self.get_key_from_kwargs(**dict((f, getattr(instance, f)) for f in self.key))

    def get_key_from_kwargs(self, **kwargs):
        """
        Return the routing key for an object given ``kwargs``.

        >>> shard_key = Model._shards.get_key_from_instance(forum_id=1)
        """
        return sum(int(kwargs[f]) for f in self.key)


class ShardOptions(object):
    def __init__(self, parent, num):
        self.parent = parent
        self.num = num
        self.model = None
        self.name = None

    def __repr__(self):
        return u'<%s: model=%s, parent=%s, num=%s>' % (
            self.__class__.__name__, self.model,
            self.parent, self.num)

    @property
    def is_child(self):
        return True

    @property
    def is_master(self):
        return False

    @property
    def key(self):
        return self.parent._shards.key

    @property
    def cluster(self):
        return self.parent._shards.cluster

    @property
    def sequence(self):
        return self.parent._shards.sequence

    def get_all_databases(self):
        """
        Returns a list of all database aliases that this shard is
        bound to.
        """
        return (self.get_database(), self.get_database(slave=True))

    def get_database(self, slave=False):
        parent = self.parent._shards
        if not parent.size:
            return
        alias = parent.cluster
        if slave:
            alias += '.slave'
        alias += '.shard%d' % (self.num % parent.size,)
        return alias

    def get_key_from_instance(self, *args, **kwargs):
        return self.parent._shards.get_key_from_instance(*args, **kwargs)

    def get_key_from_kwargs(self, *args, **kwargs):
        return self.parent._shards.get_key_from_kwargs(*args, **kwargs)

    def contribute_to_class(self, cls, name):
        self.name = name
        self.model = cls
        setattr(cls, name, self)


def generate_child_partition(parent, num):
    opts = parent._meta
    partition_name = '%s_Partition%s' % (parent.__name__, num)

    # HACK: Attempting to initialize a model twice results in a broken model
    # even though ModelBase is supposed to handle this case already.  Instead,
    # we explicitly check to make sure the model wasn't created yet by
    # using get_model to prevent this case.
    app_label = parent._meta.app_label
    m = loading.get_model(app_label, partition_name, seed_cache=False)
    if m is not None:
        return m

    partition = ModelBase(partition_name, (parent,), {
        '__module__': parent.__module__,
        'objects': Manager(),
        'Meta': type('Meta', (object,), {
            'managed': True,
            'db_table': '%s_%s' % (parent._meta.db_table, num),
            'unique_together': opts.unique_together,
        }),
        '_shards': ShardOptions(parent=parent, num=num),
    })
    partition.add_to_class('DoesNotExist', subclass_exception('DoesNotExist', (parent.DoesNotExist,), parent.__module__))
    partition.add_to_class('MultipleObjectsReturned', subclass_exception('MultipleObjectsReturned', (parent.MultipleObjectsReturned,), parent.__module__))

    # Connect signals so we can re-send them
    signaler = resend_signal(parent)
    for signal in (signals.pre_save, signals.post_save, signals.pre_delete, signals.post_delete,
                   signals.pre_init, signals.post_init, signals.m2m_changed):
        signal.connect(signaler, sender=partition, weak=False)

    # Ensure the partition is available within the module scope
    module = sys.modules[parent.__module__]
    setattr(module, partition.__name__, partition)

    # Register all partitions with Django
    loading.register_models(app_label, partition)

    return partition


class PartitionDescriptor(ModelBase):
    """
    Creates  partitions from the base model and attaches them to ``cls._shardss``.
    All children will also have ``cls._shards`` referencing the parent model.

    Also enforces the base model to be abstract, and assumes it's not a real table.
    """
    def __new__(cls, name, bases, attrs):
        # Force this model to be abstract as it's not a real table
        if 'Meta' not in attrs:
            is_abstract = False
            attrs['Meta'] = type('Meta', (object,), {
                'abstract': True,
            })
        else:
            is_abstract = getattr(attrs['Meta'], 'abstract', False)
            attrs['Meta'].abstract = True

        attrs['objects'] = MasterPartitionManager()

        new_cls = super(PartitionDescriptor, cls).__new__(cls, name, bases, attrs)

        attr_shardopts = attrs.pop('Shards', None)

        # HACK: non-abstract inheritance is not supported due to issues with metaclass
        # recursion
        if not any(b._shards.abstract if hasattr(b, '_shards') else True for b in bases):
            return new_cls

        if not attr_shardopts:
            shardopts = getattr(new_cls, 'Shards', None)
        else:
            shardopts = attr_shardopts
        base_shardopts = getattr(new_cls, '_shards', None)

        shards = []
        new_cls.add_to_class('_shards', MasterShardOptions(shardopts, nodes=shards))

        if base_shardopts:
            for k in DEFAULT_NAMES:
                if not hasattr(new_cls._shards, k):
                    setattr(new_cls._shards, k, getattr(base_shardopts, k, None))

        # We record the true abstract switch as part of _shards
        new_cls._shards.abstract = is_abstract

        if is_abstract:
            return new_cls

        # Some basic validation
        for k in DEFAULT_NAMES:
            if getattr(new_cls._shards, k, None) is None:
                raise ValidationError('Missing shard configuration value for %r on %r.' % (k, new_cls))

        new_cls.add_to_class('DoesNotExist', subclass_exception('DoesNotExist', (ObjectDoesNotExist,), new_cls.__module__))
        new_cls.add_to_class('MultipleObjectsReturned', subclass_exception('MultipleObjectsReturned', (MultipleObjectsReturned,), new_cls.__module__))

        # Because we're an abstract model, we must also fake our own registration
        app_label = new_cls._meta.app_label
        loading.register_models(app_label, new_cls)

        new_cls._really_prepare()

        # We need to create a model for each partition instance which is assigned to the
        # appropriate table
        for n in xrange(new_cls._shards.num_shards):
            partition = generate_child_partition(new_cls, n)

            # Add to list of partitions for this master
            shards.append(partition)

        return new_cls

    # Kill off default _prepare function
    _really_prepare = ModelBase._prepare
    # _prepare = lambda x: None


class PartitionModel(Model):
    __metaclass__ = PartitionDescriptor

    class Meta:
        abstract = True
