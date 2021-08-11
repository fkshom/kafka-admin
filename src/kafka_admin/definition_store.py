from .topic import *
from .acl import *
from .consumer_group import *
from .consumer_group_offsets import *


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
