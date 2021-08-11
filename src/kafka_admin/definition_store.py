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

class Acls(list):
    def __init__(self, *args) -> None:
        super().__init__(args)
        # self._headers = ['principal', 'resource_type', 'resource_name', 'pattern_type', 'operation', 'permission_type', 'host']

    def __sub__(self, other) -> Acls:
        return self.__class__(*[item for item in self if item not in other])

    def _dict_to_ACL(self, acl_dict) -> ACL:
        acl = ACL(
            principal=acl_dict['principal'].strip(),
            host=acl_dict.get('host', "*").strip(),
            operation=getattr(ACLOperation, acl_dict.get('operation', 'ALL').strip().upper()),
            permission_type=getattr(ACLPermissionType, acl_dict.get('permission_type', 'ALLOW').strip().upper()),
            resource_pattern=ResourcePattern(
                resource_type=getattr(ResourceType, acl_dict.get('resource_type', 'TOPIC').strip().upper()),
                resource_name=acl_dict.get('resource_name', "*").strip(),
                pattern_type=getattr(ACLResourcePatternType, acl_dict.get('pattern_type', 'LITERAL').strip().upper()),
            )
        )
        return acl

    def load_from_lines(self, lines) -> Acls:
        fwf = FixedWidthFormatter()
        text = ''.join(lines)
        acl_dicts = fwf.from_text(text, has_header=True).to_dict(write_header=False)
        for acl_dict in acl_dicts:
            acl = self._dict_to_ACL(acl_dict)
            self.append(acl)

        return self

    def load_from_acl_objects(self, acl_objects) -> Acls:
        for acl_object in acl_objects:
            self.append(acl_object)

        return self

    def to_csv(self) -> str:
        fwf = FixedWidthFormatter()
        acl_dicts = []
        for acl in self:
            acl_dicts.append(dict(
                principal=acl.principal,
                resource_type=acl.resource_pattern.resource_type.name,
                resource_name=acl.resource_pattern.resource_name,
                pattern_type=acl.resource_pattern.pattern_type.name,
                operation=acl.operation.name,
                permission_type=acl.permission_type.name,
                host=acl.host,
            ))

        return fwf.from_dict(acl_dicts).to_text()


class KafkaAclStoreAdapter():
    def __init__(self, client) -> None:
        self.client = client
    
    def add(self, acls):
        return self.client.create_acls(acls)
        # {'succeeded': [],
        #  'failed': [(
        #       <ACL principal=User:Alice, resource=<ResourcePattern type=TOPIC, name=*, pattern=LITERAL>,
        #           operation=ALL, type=ALLOW, host=*>,
        #       <class 'kafka.errors.SecurityDisabledError'>
        #   )]
        # }

    def delete(self, acls):
        return self.client.delete_acls(acls)
        #[(<ACL principal=User:Alice, resource=<ResourcePattern type=TOPIC, name=*, pattern=LITERAL>, operation=ALL, type=ALLOW, host=*>,
        #  [(<ACL principal=User:Alice, resource=<ResourcePattern type=TOPIC, name=*, pattern=LITERAL>, operation=ALL, type=ALLOW, host=*>,
        #    <class 'kafka.errors.NoError'>)],
        #  <class 'kafka.errors.NoError'>)]

    def list(self) -> Acls:
        acl_all_filter = ACLFilter(
            principal=None,
            host=None,
            operation=ACLOperation.ANY,
            permission_type=ACLPermissionType.ANY,
            resource_pattern=ResourcePatternFilter(
                resource_type=ResourceType.ANY,
                resource_name=None,
                pattern_type=ACLResourcePatternType.ANY,
            )
        )
        acls, error = self.client.describe_acls(acl_all_filter)
        if error != kafka.errors.NoError:
            raise Exception(error)

        return Acls().load_from_acl_objects(acls)


class DefinitionStore():
    def __init__(self) -> None:
        self.schema_version = None
        self.topics = Topics()
        self.acls = Acls()

    def load_v1(self, lines) -> None:
        # cut out topics section
        topic_lines = []
        while len(lines) > 0:
            line = lines.pop(0)
            if line.startswith('---'):
                break
            else:
                topic_lines.append(line)

        self.topics.load_from_lines(topic_lines)

        # read acls
        acl_lines = lines
        self.acls.load_from_lines(acl_lines)

    def load(self, filename) -> None:
        with open(filename, 'r') as f:
            lines = f.readlines()

        while len(lines) > 0:
            line = lines.pop(0)

            if line.startswith('---'):
                break
            elif line.startswith('schema_version:'):
                schema_version = int(line.split(':')[1].strip())
            else:
                raise Exception(f"Unknown metadata: {line}")

        if schema_version == 1:
            self.schema_version = schema_version
            self.load_v1(lines)
        else:
            raise Exception(f"Unsupported schema_version: {schema_version}")
