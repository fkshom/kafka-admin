from __future__ import annotations
import pdb
from kafka.admin.acl_resource import ACL, ACLFilter, ACLOperation, ACLPermissionType, ResourcePattern, ResourceType, ACLResourcePatternType, ResourcePatternFilter
from kafka.admin.client import KafkaAdminClient
import kafka
from kafka_admin.pyfixedwidths import FixedWidthFormatter
from pprint import pprint as pp
from kafka.admin import NewTopic

class KafkaConsumerGroupStoreAdapter():
    def __init__(self, client) -> None:
        self.client = client
    
    def list(self) -> list:
        consumer_groups = self.client.list_consumer_groups()
        consumer_group_ids = list(map(lambda x: x[0], consumer_groups))
        consumer_group_details = self.client.describe_consumer_groups(consumer_group_ids)
        return consumer_group_details

        # [GroupInformation(
        #  error_code=0, group='console-consumer-11249', state='Stable', protocol_type='consumer', protocol='range',
        #  members=[MemberInformation(
        #    member_id='consumer-console-consumer-11249-1-2f1337f6-eef3-4272-ad70-e6cf98fdb0f6',
        #    client_id='consumer-console-consumer-11249-1', client_host='/127.0.0.1',
        #    member_metadata=ConsumerProtocolMemberMetadata(version=1, subscription=['unko1'], user_data=None),
        #    member_assignment=ConsumerProtocolMemberAssignment(
        #      version=1, assignment=[(topic='unko1', partitions=[0])],
        #      user_data=None)
        #  )],
        #  authorized_operations=None)]

        # [GroupInformation(
        #  error_code=0, group='console-consumer-11249', state='Empty', protocol_type='consumer', protocol='',
        #  members=[],
        #  authorized_operations=None)]
