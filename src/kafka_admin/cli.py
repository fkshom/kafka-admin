import argparse
import os
import click
import ssl
from click.globals import pop_context
from kafka.admin.acl_resource import ACL, ACLFilter, ACLOperation, ACLPermissionType, ResourcePattern, ResourceType, ACLResourcePatternType, ResourcePatternFilter
from kafka.admin.client import KafkaAdminClient
import kafka
import yaml
from kafka_admin.definition_store import KafkaAclStoreAdapter, DefinitionStore, Acls, Topics, KafkaTopicStoreAdapter
from pprint import pprint as pp


class Config():
    def __init__(self, filename):
        self.profile = self.load(filename)

    def load(self, filename):
        with open(filename, 'r') as f:
            data = yaml.safe_load(f)
        
        default_profile = data.get('default_profile', None)
        profiles = data.get('profiles', {})
        if default_profile in profiles:
            result = profiles[ default_profile ]
        elif len(profiles.keys()) > 0:
            result = profiles[ profiles.keys()[0] ]
        else:
            raise Exception("No profile")
        
        return result

    @property
    def bootstrap_servers(self):
        return self.profile['bootstrap_servers']

    @property
    def security_protocol(self):
        return self.profile['security_protocol']

    @property
    def sasl_mechanism(self):
        return self.profile['sasl_mechanism']

    @property
    def sasl_plain_username(self):
        return self.profile['sasl_plain_username']

    @property
    def sasl_plain_password(self):
        return self.profile['sasl_plain_password']

def create_admin_client(profilename=None):
    config = Config('config.yaml')
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    admin_client = KafkaAdminClient(
        bootstrap_servers=config.bootstrap_servers,
        sasl_mechanism=config.sasl_mechanism,
        security_protocol=config.security_protocol,
        sasl_plain_username=config.sasl_plain_username,
        sasl_plain_password=config.sasl_plain_password,
        ssl_context=context,
    )
    return admin_client

@click.group()
def cmd():
    pass

@cmd.group()
def topic():
    pass

@topic.command()
def add():
    admin_client = create_admin_client()
    from pprint import pprint as pp
    from kafka.admin import NewTopic

    topics = []
    topics.append(NewTopic(name="example_topic3", num_partitions=2, replication_factor=1))
    ret = admin_client.create_topics(new_topics=topics, validate_only=False)
    pp(ret)
    # CreateTopicsResponse_v3(throttle_time_ms=0, topic_errors=[(topic='example_topic1', error_code=0, error_message=None)])

@topic.command()
def remove():
    admin_client = create_admin_client()
    from pprint import pprint as pp
    ret = admin_client.delete_topics(['example_topic1'])
    pp(ret)
    # DeleteTopicsResponse_v3(throttle_time_ms=0, topic_error_codes=[(topic='example_topic1', error_code=0)])
    # delete.topic.enable=true

@topic.command()
def list():
    admin_client = create_admin_client()
    from pprint import pprint as pp
    pp(admin_client.describe_topics())
    # [{'error_code': 0,
    #  'is_internal': False,
    #  'partitions': [{'error_code': 0,
    #                  'isr': [1],
    #                  'leader': 1,
    #                  'offline_replicas': [],
    #                  'partition': 0,
    #                  'replicas': [1]}],
    #  'topic': 'example_topic1'}]
    adapter = KafkaTopicStoreAdapter(client=admin_client)
    try:
        topics = adapter.list()
        print(topics.to_csv(verbose=True))
    except Exception as e:
        pp(e)
        click.secho(e, fg='red')

def reorder_cur_topics(new_topics, cur_topics):
    result_cur_topics = Topics()

    for new_topic in new_topics:
        try:
            cur_topics.remove(new_topic)
            result_cur_topics.append(new_topic)
        except ValueError:
            pass
    result_cur_topics.extend(sorted(cur_topics, key=lambda x: x.name))

    return result_cur_topics

@topic.command()
@click.option('--check', is_flag=True, default=False, help='check mode')
@click.option('--delete-first', is_flag=True, default=False, help='Delete first when recreate pertition')
def apply(check, delete_first):
    config = Config('config.yaml')
    store = DefinitionStore()
    store.load('definitions/sample.csv')

    admin_client = create_admin_client()
    adapter = KafkaTopicStoreAdapter(client=admin_client)

    new_topics = store.topics
    cur_topics = adapter.list()

    topics_marked_add = new_topics - cur_topics
    topics_marked_del = cur_topics - new_topics

    click.secho('Will be added', fg='green')
    print(topics_marked_add.to_csv(verbose=False))
    click.secho('Will be deleted', fg='green')
    print(topics_marked_del.to_csv(verbose=False))

    click.secho('diff', fg='green')
    cur_topics = reorder_cur_topics(new_topics, cur_topics)

    import os
    import subprocess
    import tempfile
    with tempfile.TemporaryDirectory() as dname:
        with open(os.path.join(dname, "broker.txt"), "w") as f:
            print(cur_topics.to_csv(verbose=False), file=f)
        with open(os.path.join(dname, "csv.txt"), "w") as f:
            print(new_topics.to_csv(verbose=False), file=f)

        proc = subprocess.run([
                'diff', '-u', '--color=always', '--ignore-all-space',
                os.path.join(dname, "broker.txt"), os.path.join(dname, "csv.txt")
            ],
            encoding='utf-8', stdout=subprocess.PIPE)
        print(proc.stdout)

    if check:
        click.secho('Check mode', fg='blue')
    else:
        if delete_first:
            click.secho("Result of deletion", fg='green')
            result = adapter.delete(topics_marked_del)
            pp(result)
            click.secho("Result of addition", fg='green')
            result = adapter.add(topics_marked_add)
            pp(result)
        else:
            click.secho("Result of addition", fg='green')
            result = adapter.add(topics_marked_add)
            pp(result)
            click.secho("Result of deletion", fg='green')
            result = adapter.delete(topics_marked_del)
            pp(result)

    click.secho('Finish', fg='green')

@cmd.group()
def consumer_groups():
    pass

@consumer_groups.command()
@click.option('--detail', is_flag=True, default=False, help='detail mode')
def list(detail):
    admin_client = create_admin_client()
    consumer_groups = admin_client.list_consumer_groups()

    if not detail:
        pp(consumer_groups)
    else:
        consumer_group_ids = list(map(lambda x: x.id, consumer_groups))
        consumer_group_details = admin_client.describe_consumer_groups(consumer_group_ids)
        pp(consumer_group_details)


@cmd.group()
def acl():
    pass


@acl.command()
def add():
    acl = ACL(
        principal='User:Alice',
        host="*",
        operation=ACLOperation.ALL,
        permission_type=ACLPermissionType.ALLOW,
        resource_pattern=ResourcePattern(
            resource_type=ResourceType.TOPIC,
            resource_name="*",
            pattern_type=ACLResourcePatternType.LITERAL,
        )
    )
    admin_client = create_admin_client()
    from pprint import pprint as pp
    ret = admin_client.create_acls([acl])
    if ret['failed'] != []:
        click.secho(ret['failed'], fg='red')
        import pdb; pdb.set_trace()
    else:
        click.secho(ret, fg='green')
        click.secho('Finish', fg='green')

@acl.command()
def remove():
    acl = ACLFilter(
        principal='User:Alice',
        host="*",
        operation=ACLOperation.ALL,
        permission_type=ACLPermissionType.ALLOW,
        resource_pattern=ResourcePattern(
            resource_type=ResourceType.TOPIC,
            resource_name="*",
            pattern_type=ACLResourcePatternType.LITERAL,
        )
    )
    admin_client = create_admin_client()
    from pprint import pprint as pp
    ret = admin_client.delete_acls([acl])
    pp(ret)
    if ret[0][2] != kafka.errors.NoError:
        click.secho(ret, fg='red')
        import pdb; pdb.set_trace()
    else:
        click.secho(ret, fg='green')
        click.secho('Finish', fg='green')

@acl.command()
def list():
    config = Config('config.yaml')
    admin_client = create_admin_client()
    adapter = KafkaAclStoreAdapter(client=admin_client)
    try:
        acls = adapter.list()
        pp(acls)
    except Exception as e:
        pp(e)
        click.secho(e, fg='red')


@acl.command()
def diff():
    pass


def reorder_cur_acls(new_acls, cur_acls):
    result_cur_acls = Acls()

    for new_acl in new_acls:
        try:
            cur_acls.remove(new_acl)
            result_cur_acls.append(new_acl)
        except ValueError:
            pass
    result_cur_acls.extend(cur_acls)

    return result_cur_acls

@acl.command()
@click.option('--check', is_flag=True, default=False, help='check mode')
def apply(check):
    config = Config('config.yaml')
    store = DefinitionStore()
    store.load('definitions/sample.csv')

    admin_client = create_admin_client()
    adapter = KafkaAclStoreAdapter(client=admin_client)

    new_acls = store.acls
    cur_acls = adapter.list()

    acls_marked_add = new_acls - cur_acls
    acls_marked_del = cur_acls - new_acls

    click.secho('Will be added', fg='green')
    print(acls_marked_add.to_csv())
    click.secho('Will be deleted', fg='green')
    print(acls_marked_del.to_csv())

    cur_acls = reorder_cur_acls(new_acls, cur_acls)
    click.secho('diff', fg='green')

    import os
    import subprocess
    import tempfile
    with tempfile.TemporaryDirectory() as dname:
        with open(os.path.join(dname, "broker.txt"), "w") as f:
            print(cur_acls.to_csv(), file=f)
        with open(os.path.join(dname, "csv.txt"), "w") as f:
            print(new_acls.to_csv(), file=f)

        proc = subprocess.run([
                'diff', '-u', '--color=always', '--ignore-all-space',
                os.path.join(dname, "broker.txt"), os.path.join(dname, "csv.txt")
            ],
            encoding='utf-8', stdout=subprocess.PIPE)
        print(proc.stdout)

    if check:
        click.secho('Check mode', fg='blue')
    else:
        click.secho("Result of addition", fg='green')
        result = adapter.add(acls_marked_add)
        pp(result)

        click.secho("Result of deletion", fg='green')
        result = adapter.delete(acls_marked_del)
        pp(result)

    click.secho('Finish', fg='green')

if __name__ == "__main__":
    cmd()
