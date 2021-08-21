import argparse
import os
import click
import ssl
from click.globals import pop_context
from kafka.admin.acl_resource import ACL, ACLFilter, ACLOperation, ACLPermissionType, ResourcePattern, ResourceType, ACLResourcePatternType, ResourcePatternFilter
from kafka.admin.client import KafkaAdminClient, KafkaClient
from kafka import KafkaConsumer
import kafka

from kafka_admin.definition_store import (
    KafkaTopicStoreAdapter,
    KafkaAclStoreAdapter,
    KafkaConsumerGroupStoreAdapter,
    KafkaConsumerGroupOffsetsStoreAdapter,
    DefinitionStore, Acls, Topics
)

from kafka_admin.config import Config
from pprint import pprint as pp
from types import SimpleNamespace

from logging import basicConfig, getLogger, StreamHandler, DEBUG, Formatter
logger = getLogger(__name__)
streamhandler = StreamHandler()
formatter = Formatter('[%(asctime)s][%(levelname)s][%(message)s]')
streamhandler.setFormatter(formatter)
logger.addHandler(streamhandler)
logger.setLevel(DEBUG)

import click_log
click_log.basic_config(logger)


def create_admin_client(profilename=None):
    config = Config('config.yaml')
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    admin_client = KafkaAdminClient(
        bootstrap_servers=config.bootstrap_servers,
        security_protocol=config.security_protocol,
        # sasl_mechanism=config.sasl_mechanism,
        # sasl_plain_username=config.sasl_plain_username,
        # sasl_plain_password=config.sasl_plain_password,
        # ssl_context=context,
    )
    return admin_client

def create_consumer_client(profilename=None):
    config = Config('config.yaml')
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    client = KafkaConsumer(
        bootstrap_servers=config.bootstrap_servers,
        security_protocol=config.security_protocol,
        # sasl_mechanism=config.sasl_mechanism,
        # sasl_plain_username=config.sasl_plain_username,
        # sasl_plain_password=config.sasl_plain_password,
        # ssl_context=context,
    )
    return client

@click.group()
def cmd():
    pass

@cmd.group()
def topic():
    pass

@topic.command()
@click.option('--num-partitions', default=1, help='num of partitions')
@click.option('--replication-factor', default=1, help='replication factor')
@click.argument('topicname')
def add(topicname, num_partitions, replication_factor):
    click.echo(f"topic name: {topicname}")
    click.echo(f"num of partitions: {num_partitions}")
    click.echo(f"replication factor: {replication_factor}")

    admin_client = create_admin_client()
    adapter = KafkaTopicStoreAdapter(client=admin_client)

    try:
        ret = adapter.add([SimpleNamespace(
            name=topicname,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )])
        logger.debug(ret)
        print("Success: Topic Added")
    except kafka.errors.TopicAlreadyExistsError as e:
        logger.debug(e)
        print("ERROR: Topic Already Exists.")
    return

@topic.command()
@click.argument('topicname')
def remove(topicname):
    admin_client = create_admin_client()
    adapter = KafkaTopicStoreAdapter(client=admin_client)

    try:
        ret = adapter.delete([SimpleNamespace(name=topicname)])
        logger.debug(ret)
        print("Success: Topic Deleted")
    except kafka.errors.UnknownTopicOrPartitionError as e:
        logger.debug(e)
        print("ERROR: Unknown Topic On Partition.")

@topic.command(name='list')
def list_command():
    admin_client = create_admin_client()
    adapter = KafkaTopicStoreAdapter(client=admin_client)
    try:
        topics = adapter.list()
        print(topics.to_csv(verbose=True))
    except Exception as e:
        print(e)
        click.secho(e, fg='red')

    config = Config('config.yaml')
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    client = create_consumer_client()

    print("last_positions")
    from kafka import TopicPartition
    for topic in topics:
        partition_ids = client.partitions_for_topic(topic.name)
        topic_partitions = [TopicPartition(topic.name, partition_id) for partition_id in partition_ids]
        client.assign(topic_partitions)
        client.seek_to_end()
        for topic_partition in topic_partitions:
            last_position = client.position(topic_partition)
            print(f"{topic_partition.topic} {topic_partition.partition} {last_position}")


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

@consumer_groups.command(name='list')
def list_command():
    admin_client = create_admin_client()
    adapter = KafkaConsumerGroupStoreAdapter(client=admin_client)
    consumer_group_details = adapter.list()
    logger.debug(consumer_group_details)
    consumer_group_detail_dicts = []

    for consumer_group_detail in consumer_group_details:
        if len(consumer_group_detail.members) == 0:
            consumer_group_detail_dicts.append(dict(
                error_code=consumer_group_detail.error_code,
                group=consumer_group_detail.group,
                state=consumer_group_detail.state,
                protocol_type=consumer_group_detail.protocol_type,
                protocol=consumer_group_detail.protocol,
                client_id="",
                client_host="",
                subscription="",
                topic="",
                partitions="",
            ))
        else:
            for member_info in consumer_group_detail.members:
                for assignment in member_info.member_assignment.assignment:
                    consumer_group_detail_dicts.append(dict(
                        error_code=consumer_group_detail.error_code,
                        consumer_group=consumer_group_detail.group,
                        state=consumer_group_detail.state,
                        protocol_type=consumer_group_detail.protocol_type,
                        protocol=consumer_group_detail.protocol,
                        client_id=member_info.client_id,
                        client_host=member_info.client_host,
                        topic=assignment[0],
                        partitions=str(assignment[1])
                    ))

    from kafka_admin.pyfixedwidths import FixedWidthFormatter
    fwf = FixedWidthFormatter()
    print(fwf.from_dict(consumer_group_detail_dicts).to_text())

@cmd.group()
def consumer_group_offsets():
    pass

@consumer_group_offsets.command(name='list')
def list_command():
    admin_client = create_admin_client()
    adapter = KafkaConsumerGroupOffsetsStoreAdapter(client=admin_client)
    consumer_groups_offsets_info = adapter.list()
    logger.debug(consumer_groups_offsets_info)

    consumer_group_offset_dicts = []
    for consumer_groups_offset_info in consumer_groups_offsets_info:
        consumer_group_id = consumer_groups_offset_info['consumer_group']
        consumer_group_offsets = consumer_groups_offset_info['consumer_group_offsets']

        for topic_partition, offset_and_metadata in consumer_group_offsets.items():
            consumer_group_offset_dicts.append(dict(
                consumer_group=consumer_group_id,
                topic=topic_partition.topic,
                partition=topic_partition.partition,
                offset=offset_and_metadata.offset,
                metadata=offset_and_metadata.metadata,
            ))

    from kafka_admin.pyfixedwidths import FixedWidthFormatter
    fwf = FixedWidthFormatter()
    print(fwf.from_dict(consumer_group_offset_dicts).to_text())


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
    adapter = KafkaAclStoreAdapter(client=admin_client)
    ret = adapter.add([acl])
    logger.debug(ret)
    if ret['failed'] != []:
        click.secho(ret['failed'], fg='red')
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
    adapter = KafkaAclStoreAdapter(client=admin_client)
    ret = adapter.delete([acl])
    logger.debug(ret)
    if ret[0][2] != kafka.errors.NoError:
        click.secho(ret, fg='red')
    else:
        click.secho(ret, fg='green')
        click.secho('Finish', fg='green')

@acl.command(name='list')
def list_command():
    config = Config('config.yaml')
    admin_client = create_admin_client()
    adapter = KafkaAclStoreAdapter(client=admin_client)
    try:
        acls = adapter.list()
        print(acls)
    except Exception as e:
        print(e)
        click.secho(e, fg='red')


@acl.command()
def diff():
    pass

@acl.command()
@click.option('--check', is_flag=True, default=False, help='check mode')
def clear(check):

    admin_client = create_admin_client()
    adapter = KafkaAclStoreAdapter(client=admin_client)

    store = DefinitionStore()
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
    # basicConfig(level=DEBUG)
    cmd()
