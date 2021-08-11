from __future__ import annotations
import pdb
from kafka.admin.acl_resource import ACL, ACLFilter, ACLOperation, ACLPermissionType, ResourcePattern, ResourceType, ACLResourcePatternType, ResourcePatternFilter
from kafka.admin.client import KafkaAdminClient
import kafka
from kafka_admin.pyfixedwidths import FixedWidthFormatter
from pprint import pprint as pp
from kafka.admin import NewTopic

class Topic():
    def __init__(self, name=None, num_partitions=None, replication_factor=None, raw=None):
        self.name = name
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self._raw = raw

    def __eq__(self, other) -> bool:
        return all((
            self.name == other.name,
            self.num_partitions == other.num_partitions,
            self.replication_factor == other.replication_factor,
        ))

    def __hash__(self):
        return hash((
            self.name,
            self.num_partitions,
            self.replication_factor,
        ))

class Topics(list):
    def __init__(self, *args) -> None:
        super().__init__(args)

    def __sub__(self, other) -> Topics:
        return self.__class__(*[item for item in self if item not in other])

    def _dict_to_Topic(self, topic_dict) -> Topic:
        topic = Topic(
            name=topic_dict['name'].strip(),
            num_partitions=int(topic_dict.get('num_partitions', 1)),
            replication_factor=int(topic_dict.get('replication_factor', 1)),
        )
        return topic

    def load_from_lines(self, lines) -> Topics:
        fwf = FixedWidthFormatter()
        text = ''.join(lines)
        topic_dicts = fwf.from_text(text, has_header=True).to_dict(write_header=False)
        for topic_dict in topic_dicts:
            topic = self._dict_to_Topic(topic_dict)
            self.append(topic)

        return self

    def load_from_topic_objects(self, topic_objects) -> Topics:
        # [{'error_code': 0,
        #  'is_internal': False,
        #  'partitions': [{'error_code': 0,
        #                  'isr': [1],
        #                  'leader': 1,
        #                  'offline_replicas': [],
        #                  'partition': 0,
        #                  'replicas': [1]}],
        #  'topic': 'example_topic1'}]
        for topic_object in topic_objects:
            topic = Topic(
                name=topic_object['topic'],
                num_partitions=len(topic_object['partitions']),
                replication_factor=len(topic_object['partitions'][0]['replicas']),
                raw=topic_object,
            )
            self.append(topic)
        return self

    def to_csv(self, verbose=True) -> str:
        fwf = FixedWidthFormatter()
        topic_dicts = []
        for topic in self:
            if verbose:
                for idx, partition in enumerate(sorted(topic._raw['partitions'], key=lambda x: x['partition'])):
                    topic_dicts.append(dict(
                        name=topic.name if idx == 0 else "",
                        num_partitions=topic.num_partitions if idx == 0 else "",
                        replication_factor=topic.replication_factor if idx == 0 else "",
                        partition_id=partition['partition'],
                        leader=partition['leader'],
                        # replicas=f"[{ ','.join(list(map(str, partition['replicas']))) }]",
                        # isr=f"[{ ','.join(list(map(str, partition['isr']))) }]",
                        # offline_replicas=f"[{ ','.join(list(map(str, partition['offline_replicas']))) }]",
                        replicas=str(partition['replicas']),
                        isr=str(partition['isr']),
                        offline_replicas=str(partition['offline_replicas']),
                        error_code=partition['error_code'],
                    ))
            else:
                topic_dicts.append(dict(
                    name=topic.name,
                    num_partitions=topic.num_partitions,
                    replication_factor=topic.replication_factor,
                ))

        return fwf.from_dict(topic_dicts).to_text()

class KafkaTopicStoreAdapter():
    def __init__(self, client) -> None:
        self.client = client

    def list(self) -> Topics:
        topics = self.client.describe_topics(topics=None)
        return Topics().load_from_topic_objects(topics)
        # [{'error_code': 0,
        #  'is_internal': False,
        #  'partitions': [{'error_code': 0,
        #                  'isr': [1],
        #                  'leader': 1,
        #                  'offline_replicas': [],
        #                  'partition': 0,
        #                  'replicas': [1]}],
        #  'topic': 'example_topic1'}]
        
    def add(self, topics):
        new_topics = []
        for topic in topics:
            new_topics.append(NewTopic(
                name=topic.name,
                num_partitions=topic.num_partitions,
                replication_factor=topic.replication_factor
            ))
        return self.client.create_topics(new_topics=new_topics, validate_only=False)
        # CreateTopicsResponse_v3(throttle_time_ms=0, topic_errors=[(topic='example_topic1', error_code=0, error_message=None)])

    def delete(self, topics):
        delete_topic_names = []
        for topic in topics:
            delete_topic_names.append(topic.name)
        return self.client.delete_topics(delete_topic_names)
        # DeleteTopicsResponse_v3(throttle_time_ms=0, topic_error_codes=[(topic='example_topic1', error_code=0)])
        # delete.topic.enable=true