from __future__ import annotations
import pdb
from kafka.admin.acl_resource import ACL, ACLFilter, ACLOperation, ACLPermissionType, ResourcePattern, ResourceType, ACLResourcePatternType, ResourcePatternFilter
from kafka.admin.client import KafkaAdminClient
import kafka
from kafka_admin.pyfixedwidths import FixedWidthFormatter
from pprint import pprint as pp
from kafka.admin import NewTopic

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