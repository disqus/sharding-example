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

from django.db import transaction, router, IntegrityError
from django.db.models.manager import Manager
from django.db.models.query import QuerySet, ValuesQuerySet, ValuesListQuerySet


class PartitionQuerySetBase(object):
    @property
    def db(self):
        if self._db:
            return self._db

        if self._for_write:
            return router.db_for_write(self.model, exact_lookups=self._exact_lookups,
                                                   instance=getattr(self, '_instance', None))
        return router.db_for_read(self.model, exact_lookups=self._exact_lookups)


class PartitionQuerySet(PartitionQuerySetBase, QuerySet):
    """
    QuerySet which helps partitioning by field in the database routers by
    providing hints about what fields are being queried against (in
    ``exact_lookups``).

    If ``actual_model`` is passed, it will be used to reraise exceptions rather
    than ``model``.
    """
    def __init__(self, model=None, actual_model=None, *args, **kwargs):
        super(PartitionQuerySet, self).__init__(model=model, *args, **kwargs)
        self.actual_model = actual_model or model
        self._exact_lookups = {}

    def __getitem__(self, *args, **kwargs):
        try:
            return super(PartitionQuerySet, self).__getitem__(*args, **kwargs)
        except self.model.DoesNotExist, e:
            raise self.actual_model.DoesNotExist(unicode(e).replace(self.model.__name__, self.actual_model.__name__))

    def _clone(self, klass=None, *args, **kwargs):
        if klass is QuerySet:
            klass = PartitionQuerySet
        if klass is ValuesQuerySet:
            klass = PartitionValuesQuerySet
        elif klass is ValuesListQuerySet:
            klass = PartitionValuesListQuerySet
        clone = super(PartitionQuerySet, self)._clone(klass, *args, **kwargs)
        clone._exact_lookups = self._exact_lookups.copy()
        return clone

    def _filter_or_exclude(self, *args, **kwargs):
        clone = super(PartitionQuerySet, self)._filter_or_exclude(*args, **kwargs)
        if getattr(clone, '_exact_lookups', None) is None:
            clone._exact_lookups = {}
        clone._exact_lookups.update(dict([(k, v) for k, v in kwargs.items() if '__' not in k]))
        return clone

    def create(self, **kwargs):
        """
        This is a copy of QuerySet.create, except we save the instance we're
        about to save for the db_for_write router.  This can't use super()
        since we'll either be too early (before the instance is created) or
        too late (after the ``db`` property is hit).
        """
        obj = self.model(**kwargs)
        self._for_write = True
        self._instance = obj
        obj.save(force_insert=True, using=self.db)
        return obj

    def get(self, *args, **kwargs):
        try:
            return super(PartitionQuerySet, self).get(*args, **kwargs)
        except self.model.DoesNotExist, e:
            raise self.actual_model.DoesNotExist(unicode(e).replace(self.model.__name__, self.actual_model.__name__))

    def get_or_create(self, **kwargs):
        """
        This is a copy of QuerySet.get_or_create, that forces calling our custom
        create method when the get fails.
        """
        assert kwargs, \
                'get_or_create() must be passed at least one keyword argument'
        defaults = kwargs.pop('defaults', {})
        try:
            self._for_write = True
            return self.get(**kwargs), False
        except self.actual_model.DoesNotExist:
            params = dict([(k, v) for k, v in kwargs.items() if '__' not in k])
            params.update(defaults)
            obj = self.model(**params)
            self._for_write = True
            self._instance = obj
            using = self.db
            try:
                sid = transaction.savepoint(using=using)
                obj.save(force_insert=True, using=using)
            except IntegrityError, e:
                transaction.savepoint_rollback(sid, using=using)
                try:
                    return self.get(**kwargs), False
                except self.actual_model.DoesNotExist, e:
                    raise self.actual_model.DoesNotExist(unicode(e).replace(self.model.__name__, self.actual_model.__name__))
            else:
                transaction.savepoint_commit(sid, using=using)
                return obj, True


def partition_query_set_factory(klass):
    class _PartitionQuerySetFromFactory(PartitionQuerySetBase, klass):
        def _clone(self, klass=None, *args, **kwargs):
            clone = super(_PartitionQuerySetFromFactory, self)._clone(klass, *args, **kwargs)
            clone._exact_lookups = self._exact_lookups.copy()
            return clone

    return _PartitionQuerySetFromFactory

PartitionValuesQuerySet = partition_query_set_factory(ValuesQuerySet)
PartitionValuesListQuerySet = partition_query_set_factory(ValuesListQuerySet)


class PartitionManager(Manager):
    def get_query_set(self):
        return PartitionQuerySet(model=self.model)


class MasterPartitionManager(Manager):
    """
    Allows operation of partitions by passing key to get_query_set().
    """
    def shard(self, key, slave=False):
        """
        Given a key, which is defined by the partition and used to route queries, returns a QuerySet
        that is bound to the correct shard.

        >>> shard(343)

        >>> shard(343, slave=True)
        """
        queryset = self.get_query_set(key)
        return queryset.using(self.get_database_from_key(key, slave=slave))

    def get_database(self, shard, slave=False):
        """
        Given a shard (numeric index value), returns the correct database alias to query against.

        If ``slave`` is True, returns a read-slave.
        """
        try:
            model = self.model._shards.nodes[shard]
        except IndexError:
            raise ValueError('Shard %r does not exist on %r' % (shard, self.model.__name__))
        return model._shards.get_database(slave=slave)

    def get_database_from_key(self, key, slave=False):
        """
        Given a key, which is defined by the partition and used to route queries, returns the
        database connection alias which the data lives on.
        """
        return self.get_database(key % self.model._shards.num_shards, slave=slave)

    def get_model_from_key(self, key):
        """
        Given a key, which is defined by the partition and used to route queries, returns the
        Model which represents the shard.
        """
        shards = self.model._shards
        return shards.nodes[key % shards.num_shards]

    def get_query_set(self, key=None):
        shards = self.model._shards

        assert key is not None, 'You must filter on %s before expanding a QuerySet on %s models.' % (
            shards.key, self.model.__name__)

        model = self.get_model_from_key(key)

        return PartitionQuerySet(model=model, actual_model=self.model)

    def _wrap(func_name):
        def wrapped(self, **kwargs):
            shards = self.model._shards
            try:
                key = shards.get_key_from_kwargs(**kwargs)
            except KeyError:
                raise AssertionError('You must filter on %s before expanding a QuerySet on %s models.' % (
                    shards.key, self.model.__name__))

            return getattr(self.get_query_set(key=int(key)), func_name)(**kwargs)

        wrapped.__name__ = func_name
        return wrapped

    filter = _wrap('filter')
    get = _wrap('get')
    create = _wrap('create')
    get_or_create = _wrap('get_or_create')
