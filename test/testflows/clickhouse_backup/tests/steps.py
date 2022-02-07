import re
import random
import string
import datetime

from testflows.core import *
from testflows.asserts import values, error, snapshot
from clickhouse_backup.tests.common import random_datetime


@TestStep
def drop_table(self, node, table_name, database="default", sync=True):
    """Helper function, drops the ClickHouse table from the given node.
    """
    with When(f"dropping table {table_name} from database {database}"):
        node.query(f"DROP TABLE IF EXISTS {database}.{table_name}{' SYNC' if sync else ''}")


@TestStep(Given)
def create_table(self, node, table_name, columns, database="default", engine="MergeTree", order_by=None, sign=None, version=None,
                 config_section="graphite_rollup_params", settings=None):
    """Helper step, creates a ClickHouse table in the given node.
    """
    with When(f"creating table {table_name} with {engine} engine in database {database}"):
        schema = ""
        for i, j in columns.items():
            schema += f"{i} {j}, "

        schema = schema [:-2]

        if not order_by:
            order_by = str(list(columns.keys())[0])
        if not sign:
            sign = str(list(columns.keys())[1])
        if not version:
            version = str(list(columns.keys())[2])

        engine_params = ""

        if "VersionedCollapsingMergeTree" in engine:
            engine_params = f"{sign}, {version}"

        elif "CollapsingMergeTree" in engine:
            engine_params = f"{sign}"

        elif "GraphiteMergeTree" in engine:
            engine_params = f"'{config_section}'"

        if "Replicated" in engine:
            zoo_path = "/clickhouse/tables/{shard}" \
                       f"/{table_name}"
            engine_params = f"'{zoo_path}', '{{replica}}'" if engine_params == "" else f"'{zoo_path}', '{{replica}}', {engine_params}"


        query = f"CREATE TABLE {database}.{table_name} ({schema}) Engine = {engine}({engine_params}) ORDER BY {order_by}"
        if settings:
            query += f" SETTINGS {settings}"

        with By("execute CREATE TABLE query"):
            node.query(query)


@TestStep
def insert_into_table(self, node, table_name, values, database="default",  columns="(*)"):
    """Helper function, inserts given data into a ClickHouse table in the given node.
    """
    with When(f"inserting into table {table_name}"):
        node.query(f"INSERT INTO {database}.{table_name} {columns} VALUES {values} ")


@TestStep
def populate_table(self, node, table_name, columns, database="default", size=10, native=True):
    """Helper function, inserts given data into a ClickHouse table in the given node.
    """
    if not native:
        with When(f"populate table {table_name} with random generated data"):
            letters = string.ascii_lowercase + string.ascii_uppercase + string.ascii_letters + string.digits
            values = []

            for i in range(size):
                portion = []

                for col_name, col_type in columns.items():
                    if col_type == "String":
                        portion.append((''.join(random.choice(letters) for j in range(10))))
                    elif "Int" in col_type:
                        portion.append(random.randint(1, 51) if col_name != "Sign" else random.choice((1, -1)))
                    elif col_type == "DateTime":
                        d1 = datetime.datetime(1980, 1, 1)
                        d2 = datetime.datetime(2030, 12, 31)
                        portion.append(str(random_datetime(d1, d2)))

                values.append('(' + str(portion)[1:-1] + ')')

            values = str(values)[1:-1].replace("\"(", "(").replace(")\"", ")").replace("\"", "'")
            column_selector = str(list(columns.keys())).replace('\'', '')[1:-1]

            insert_into_table(node=node, table_name=table_name, database=database, values=values, columns=f"({column_selector})")
    else:
        random_schema = []
        insert_columns = []
        for i, j in columns.items():
            if not ("Map" in j or "LowCardinality" in j or "Nested" in j or (type(j) == str and j.startswith("Aggregate"))):
                insert_columns.append(i)
                if "'" in j:
                    j_mod = j.replace("'", "\\'")
                    random_schema.append(f"{i} {j_mod}")
                else:
                    random_schema.append(f"{i} {j}")

        str_random_schema = ", ".join(random_schema)
        str_insert_columns = ", ".join(insert_columns)

        node.query(f"INSERT INTO {database}.{table_name} ({str_insert_columns}) SELECT * FROM generateRandom('{str_random_schema}', NULL, 10, 2) LIMIT {size}")


@TestStep(Given)
def create_and_populate_table(self, node, table_name, database="default", columns=None, engine="MergeTree",
                              size=10, settings=None, native=True):
    """Helper function combining table creation and population.
    """
    if not columns:
        columns = self.context.columns

    try:
        create_table(node=node, table_name=f"{table_name}", database=database, engine=engine, columns=columns, settings=settings)
        populate_table(node=node, table_name=f"{table_name}", database=database, columns=columns, size=size, native=native)
        yield

    finally:
        with Finally("remove created table"):
            drop_table(node=node, database=database, table_name=table_name)


@TestStep(Given)
def delete_any_old_topic_and_consumer_group(self, kafka_node, bootstrap_server, topic, consumer_group, node="kafka1"):
    """Delete any old topic and consumer group.
    """
    with By("deleting topic"):
        command = (f"kafka-topics "
               f"--bootstrap-server {bootstrap_server} --delete --topic {topic}")
        kafka_node.cmd(command)

    with By("deleting consumer group"):
        command = (f"kafka-consumer-groups "
               f"--bootstrap-server {bootstrap_server} --delete --group {consumer_group}")
        kafka_node.cmd(command)


@TestStep(Given)
def create_topic(self, kafka_node, bootstrap_server, topic, consumer_group, replication_factor, partitions, node="kafka1"):
    """Create Kafka topic.
    """
    try:
        command = (f"kafka-topics --create --bootstrap-server {bootstrap_server} "
                   f"--replication-factor {replication_factor} --partitions {partitions} --topic {topic}")
        kafka_node.cmd(command, exitcode=0)

        yield topic

    finally:
        with Finally("I cleanup Kafka topic and consumer group"):
            command = (f"kafka-topics "
                f"--bootstrap-server {bootstrap_server} --delete --topic {topic}")
            kafka_node.cmd(command)

            command = (f"kafka-consumer-groups "
                f"--bootstrap-server {bootstrap_server} --delete --group {consumer_group}")
            kafka_node.cmd(command)
