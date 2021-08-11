from __future__ import annotations
import pdb
from kafka.admin.acl_resource import ACL, ACLFilter, ACLOperation, ACLPermissionType, ResourcePattern, ResourceType, ACLResourcePatternType, ResourcePatternFilter
from kafka.admin.client import KafkaAdminClient
import kafka
from kafka_admin.pyfixedwidths import FixedWidthFormatter
from pprint import pprint as pp
from kafka.admin import NewTopic

class KafkaConsumerGroupOffsetsStoreAdapter():
    def __init__(self, client) -> None:
        self.client = client
    
    def list(self) -> list:
        consumer_groups = self.client.list_consumer_groups()
        consumer_group_ids = list(map(lambda x: x[0], consumer_groups))

        consumer_groups_offsets = []
        for consumer_group_id in consumer_group_ids:
            consumer_groups_offsets.append(dict(
                consumer_group=consumer_group_id,
                consumer_group_offsets=self.client.list_consumer_group_offsets(consumer_group_id),
            ))
        # {consumer_group="consumer-group-1", 
        #  consumer_group_offsets={TopicPartition(topic='unko1', partition=0): OffsetAndMetadata(offset=5, metadata='')}
        # }

        return consumer_groups_offsets
