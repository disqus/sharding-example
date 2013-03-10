from django.db import models
from sqlshards.db.shards.models import PartitionModel
from sqlshards.db.shards.helpers import replace_pk


class SimpleModel(models.Model):
    pass


class PartitionedModel(PartitionModel):
    key = models.PositiveIntegerField()

    class Shards:
        key = 'key'
        num_shards = 2
        cluster = 'sharded'


class TestModel(PartitionModel):
    key = models.IntegerField()
    foo = models.CharField(null=True, max_length=32)

    class Shards:
        key = 'key'
        num_shards = 2
        cluster = 'sharded'

    class Meta:
        unique_together = (('key', 'foo'),)


class CompositeTestModel(PartitionModel):
    key = models.IntegerField()
    foo = models.IntegerField()

    class Shards:
        key = ('key', 'foo')
        num_shards = 2
        cluster = 'sharded'

    class Meta:
        unique_together = (('key', 'foo'),)
