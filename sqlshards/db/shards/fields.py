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

from django.db import connections, transaction
from django.db.models.fields import AutoField, BigIntegerField
from django.db.models.signals import post_syncdb, class_prepared
from django.db.utils import DatabaseError
from django.utils.translation import ugettext_lazy as _


class AutoSequenceField(BigIntegerField):
    """
    A ``BigIntegerField`` that increments using an external PostgreSQL sequence
    generator.  Used for primary keys on partitioned tables that require a
    canonical source for unique IDs.

        ``db_alias`` is the alias to the Django connection to the database
        containing the sequence.

        ``sequence`` is the string representation of the sequence table.

    """

    description = _("Integer")

    def __init__(self, db_alias, sequence=None, *args, **kwargs):
        self.db_alias = db_alias
        self.sequence = sequence

        kwargs['blank'] = True
        kwargs['editable'] = False
        kwargs['unique'] = True
        super(AutoSequenceField, self).__init__(*args, **kwargs)

    def set_sequence_name(self, **kwargs):
        self._sequence = self.sequence or '%s_%s_seq' % (self.model._meta.db_table, self.column)

    def create_sequence(self, created_models, **kwargs):
        if self.model not in created_models:
            return

        if not getattr(self, '_sequence', None):
            return

        # if hasattr(self.model, '_shards') and hasattr(self.model._shards, 'parent') and self.model._shards.parent not in created_models:
        #     return

        cursor = connections[self.db_alias].cursor()
        sid = transaction.savepoint(self.db_alias)
        try:
            cursor.execute("CREATE SEQUENCE %s;" % self._sequence)
        except DatabaseError:
            transaction.savepoint_rollback(sid, using=self.db_alias)
            # Sequence must already exist, ensure it gets reset
            cursor.execute("SELECT setval('%s', 1, false)" % (self._sequence,))
        else:
            print 'Created sequence %r on %r' % (self._sequence, self.db_alias)
            transaction.savepoint_commit(sid, using=self.db_alias)
        cursor.close()

    def contribute_to_class(self, cls, name):
        super(AutoSequenceField, self).contribute_to_class(cls, name)
        # parent models still call this method, but dont need sequences
        post_syncdb.connect(self.create_sequence, dispatch_uid='create_sequence_%s_%s' % (cls._meta, name), weak=False)
        class_prepared.connect(self.set_sequence_name, sender=cls, weak=False)

    def pre_save(self, model_instance, add):
        value = getattr(model_instance, self.attname, None)
        if add and not value:
            value = self.get_next_value()
            setattr(model_instance, self.attname, value)
        return value

    def south_field_triple(self):
        "Returns a suitable description of this field for South."
        from south.modelsinspector import introspector
        field_class = "django.db.models.fields.PositiveIntegerField"
        args, kwargs = introspector(self)
        return (field_class, args, kwargs)

    def get_next_value(self):
        cursor = connections[self.db_alias].cursor()
        try:
            cursor.execute("SELECT NEXTVAL(%s)", (self._sequence,))
            return cursor.fetchone()[0]
        finally:
            cursor.close()


class ShardedAutoField(AutoField):
    def db_type(self, *args, **kwargs):
        if not hasattr(self.model, '_shards'):
            raise ValueError("ShardedAutoField must be used with a PartitionModel.")

        if self.model._shards.is_master:
            return "bigint"

        return "bigint DEFAULT next_sharded_id('%s', %d)" % (
            get_sharded_id_sequence_name(self.model),
            self.model._shards.num)

    def create_sequence(self, created_models, **kwargs):
        # Sequence creation for production is handled by DDL scripts
        # (sqlpartition).  This is needed to create sequences for
        # test models.
        if self.model not in created_models:
            return

        db_alias = self.model._shards.cluster

        for child in self.model._shards.nodes:
            cursor = connections[db_alias].cursor()
            sid = transaction.savepoint(db_alias)
            sequence_name = get_sharded_id_sequence_name(child)
            try:
                cursor.execute("CREATE SEQUENCE %s;" % sequence_name)
            except DatabaseError:
                transaction.savepoint_rollback(sid, using=db_alias)
                # Sequence must already exist, ensure it gets reset
                cursor.execute("SELECT setval('%s', 1, false)" % (sequence_name,))
            else:
                print 'Created sequence %r on %r' % (sequence_name, db_alias)
                transaction.savepoint_commit(sid, using=db_alias)
            cursor.close()
