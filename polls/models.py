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

import datetime
from django.db import models
from django.utils import timezone
from sqlshards.db.shards.models import ShardedAutoField, PartitionModel

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
    id = ShardedAutoField(primary_key=True, auto_created=True)
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
