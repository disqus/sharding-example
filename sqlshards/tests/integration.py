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

from unittest import TestCase as UnitTestCase
from django.db.models import signals
from django.test import TestCase
from sqlshards.db.shards.helpers import get_canonical_model, is_partitioned

from .sample.models import SimpleModel, PartitionedModel, PartitionedModel_Partition0, \
                           TestModel, CompositeTestModel


class CompositeKeyShardTest(TestCase):
    def test_get_key_from_kwargs(self):
        self.assertEqual(CompositeTestModel._shards.get_key_from_kwargs(key=1, foo=2), 3)

    def test_get_key_from_instance(self):
        inst = TestModel(key=1, foo=2)
        self.assertEqual(CompositeTestModel._shards.get_key_from_instance(inst), 3)


class PartitionShardTest(TestCase):
    def test_get_database_master(self):
        node = TestModel._shards.nodes[0]
        self.assertEqual(node._shards.get_database(), 'sharded.shard0')

    def test_get_database_slave(self):
        node = TestModel._shards.nodes[0]
        self.assertEqual(node._shards.get_database(slave=True), 'sharded.slave.shard0')

    def test_get_all_databases(self):
        node = TestModel._shards.nodes[0]
        self.assertEqual(node._shards.get_all_databases(), ('sharded.shard0', 'sharded.slave.shard0'))

    def test_get_key_from_kwargs(self):
        self.assertEqual(TestModel._shards.get_key_from_kwargs(key=1), 1)

    def test_get_key_from_instance(self):
        inst = TestModel(key=1)
        self.assertEqual(TestModel._shards.get_key_from_instance(inst), 1)

    def test_shardinfo(self):
        self.assertTrue(hasattr(TestModel, '_shards'))
        self.assertTrue(hasattr(TestModel._shards, 'is_master'))
        self.assertTrue(TestModel._shards.is_master)
        self.assertTrue(hasattr(TestModel._shards, 'is_child'))
        self.assertFalse(TestModel._shards.is_child)
        self.assertTrue(hasattr(TestModel._shards, 'nodes'))
        self.assertNotEquals(len(TestModel._shards.nodes), 0)
        self.assertTrue(hasattr(TestModel._shards, 'num_shards'))
        self.assertNotEquals(TestModel._shards.num_shards, 0)
        self.assertTrue(hasattr(TestModel._shards, 'key'))
        self.assertEqual(TestModel._shards.key, ('key',))
        self.assertTrue(hasattr(TestModel._shards, 'sequence'))
        self.assertEqual(TestModel._shards.sequence, 'sample_testmodel')
        self.assertTrue(hasattr(TestModel._shards, 'cluster'))
        self.assertEqual(TestModel._shards.cluster, 'sharded')

        node1 = TestModel._shards.nodes[0]
        self.assertTrue(hasattr(node1, '_shards'))
        self.assertTrue(hasattr(node1._shards, 'is_master'))
        self.assertFalse(node1._shards.is_master)
        self.assertTrue(hasattr(node1._shards, 'is_child'))
        self.assertTrue(node1._shards.is_child)
        self.assertTrue(hasattr(node1._shards, 'parent'))
        self.assertEqual(node1._shards.parent, TestModel)
        self.assertTrue(hasattr(node1._shards, 'num'))
        self.assertEqual(node1._shards.num, 0)
        self.assertTrue(hasattr(TestModel._shards, 'cluster'))
        self.assertEqual(TestModel._shards.cluster, 'sharded')


class PartitionTest(TestCase):
    def test_get_database_from_key(self):
        self.assertEqual(TestModel.objects.get_database_from_key(2), 'sharded.shard0')
        self.assertEqual(TestModel.objects.get_database_from_key(3), 'sharded.shard1')

    def test_get_database_from_key_slave(self):
        self.assertEqual(TestModel.objects.get_database_from_key(2, slave=True), 'sharded.slave.shard0')
        self.assertEqual(TestModel.objects.get_database_from_key(3, slave=True), 'sharded.slave.shard1')

    def test_get_model_from_key(self):
        self.assertEqual(TestModel.objects.get_model_from_key(2), TestModel._shards.nodes[0])
        self.assertEqual(TestModel.objects.get_model_from_key(3), TestModel._shards.nodes[1])

    def test_get_database(self):
        self.assertEqual(TestModel.objects.get_database(0), 'sharded.shard0')
        self.assertEqual(TestModel.objects.get_database(1), 'sharded.shard1')

    def test_get_database_slave(self):
        self.assertEqual(TestModel.objects.get_database(0, slave=True), 'sharded.slave.shard0')
        self.assertEqual(TestModel.objects.get_database(1, slave=True), 'sharded.slave.shard1')

    def test_get_database_invalid_shard(self):
        self.assertRaises(ValueError, TestModel.objects.get_database, 2)
        self.assertRaises(ValueError, TestModel.objects.get_database, 2, slave=True)

    def test_routing_missing_key(self):
        self.assertRaises(AssertionError, TestModel.objects.filter, value='bar')

    def test_shard_with_valid_key(self):
        queryset = TestModel.objects.shard(0)
        self.assertEqual(queryset.model, TestModel._shards.nodes[0])

        queryset = TestModel.objects.shard(1)
        self.assertEqual(queryset.model, TestModel._shards.nodes[1])

    def test_missing_key_on_query(self):
        self.assertRaises(AssertionError, TestModel.objects.all)

    def test_module_imports(self):
        from sample import models  # NOQA
        self.assertTrue('TestModel_Partition0' in dir(models), dir(models))

    def test_get_model_on_slaves(self):
        # Ensure we're registered as part of the app's models (both abstract and partitions)
        from django.db.models import get_model
        result = get_model('sample', 'testmodel_partition0', only_installed=False)
        self.assertNotEquals(result, None)
        self.assertEqual(result.__name__, 'TestModel_Partition0')

    def test_get_model_on_parent(self):
        # Ensure we're registered as part of the app's models (both abstract and partitions)
        from django.db.models import get_model
        result = get_model('sample', 'testmodel', only_installed=False)
        self.assertEqual(result, TestModel)


class IsPartitionedTestCase(UnitTestCase):
    def test(self):
        self.assertFalse(is_partitioned(SimpleModel))
        self.assertTrue(is_partitioned(PartitionedModel))
        self.assertTrue(is_partitioned(PartitionedModel_Partition0))


class GetCanonicalModelTestCase(UnitTestCase):
    def test(self):
        self.assertEqual(get_canonical_model(SimpleModel), SimpleModel)
        self.assertEqual(get_canonical_model(PartitionedModel), PartitionedModel)
        self.assertEqual(get_canonical_model(PartitionedModel_Partition0), PartitionedModel)
