import datetime
from django.db import models
from django.utils import timezone
from sqlshards.db.shards.models import PartitionAutoSequenceField, PartitionModel

class Poll(models.Model):
    def __unicode__(self):
        return self.question

    def was_published_recently(self):
        now = timezone.now()
        return now - datetime.timedelta(days = 1) <= self.pub_date < now

    was_published_recently.admin_order_field = 'pub_date'
    was_published_recently.boolean = True
    was_published_recently.short_description = 'Published recently?'

    question = models.CharField(max_length=200)
    pub_date = models.DateTimeField('date published')


class PollPartitionBase(PartitionModel):
    id = PartitionAutoSequenceField('sharded', primary_key=True)
    poll_id = models.PositiveIntegerField(db_index=True)

    class Meta:
        abstract = True

    class Shards:
        key = 'poll_id'
        num_shards = 2
        cluster = 'sharded'


class Choice(PollPartitionBase):
    def __unicode__(self):
        return self.choice_text

    choice_text = models.CharField(max_length=200)
    votes = models.IntegerField()
