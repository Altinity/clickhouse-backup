import random

from testflows.core import *
from testflows.asserts import *

from clickhouse_backup.requirements.requirements import *
from clickhouse_backup.tests.common import *
from clickhouse_backup.tests.steps import *


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_TableEngines_OtherEngines("1.0")
)
def all_engines(self):
    """Test that tables with other engine types schema can be backed up and restored.
    """
    clickhouse = self.context.nodes[0]
    backup = self.context.backup
    name_prefix = "bp_oe"
    table_names = ["generate_random", "distributed", "merge", "join", "buffer", "dict", "file", "null", "set",
                   "url", "memory", "stripelog", "tinylog", "log", "jdbc", "odbc", "mongo", "hdfs", "s3", "embrdb",
                   "mysql", "postgres", "ext_dist", "sqlite", "rabbitmq"]
    table_schemas = {}

    create_table_queries = ["CREATE TABLE log ENGINE = Log AS SELECT toUInt32(number) as id from numbers(10)",
                            "CREATE TABLE tinylog ENGINE = TinyLog AS SELECT toUInt32(number) as id from numbers(10)",
                            "CREATE TABLE stripelog ENGINE = StripeLog AS SELECT toUInt32(number) as id from numbers(10)",
                            "CREATE TABLE memory ENGINE = Memory AS SELECT toUInt32(number) as id from numbers(10)",
                            "CREATE TABLE url (word String, value UInt64) ENGINE=URL('http://127.0.0.1:12345/', CSV)",
                            "CREATE TABLE set ENGINE = Set AS SELECT toUInt32(number) as id from numbers(10)",
                            "CREATE TABLE null ENGINE = Null AS SELECT toUInt32(number) as id from numbers(10)",
                            "CREATE TABLE file (name String, value UInt32) ENGINE=File(MyFile)",

                            "CREATE TABLE table_for_dict (key_column UInt64, third_column String) ENGINE = MergeTree() ORDER BY key_column",
                            "INSERT INTO table_for_dict select number, concat('Hello World ', toString(number)) from numbers(100)",
                            "CREATE DICTIONARY ndict(key_column UInt64 DEFAULT 0, third_column String DEFAULT 'qqq') "
                            "PRIMARY KEY key_column "
                            "SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'table_for_dict' PASSWORD '' DB 'default')) "
                            "LIFETIME(MIN 1 MAX 10) LAYOUT(HASHED())",
                            "CREATE TABLE dict (key_column UInt64, third_column String) Engine = Dictionary(ndict)",

                            "CREATE TABLE buffer AS log ENGINE = Buffer(default, log, 16, 10, 100, 10000, 1000000, 10000000, 100000000)",
                            "CREATE TABLE join (id UInt32, val UInt8) ENGINE = Join(ANY, LEFT, id)",
                            "CREATE TABLE merge (id UInt32) ENGINE = Merge(currentDatabase(), 'log')",
                            "CREATE TABLE distributed AS log ENGINE = Distributed(replicated_cluster, default, log)",
                            "CREATE TABLE generate_random (name String, value UInt32) ENGINE = GenerateRandom(1, 5, 3)",

                            "CREATE TABLE jdbc (id Int32, name String) ENGINE=JDBC('jdbc:mysql://localhost:3306/?user=root&password=root', 'test', 'test')",
                            "CREATE TABLE odbc (id Int32, name String) ENGINE=ODBC('DSN=mysqlconn', 'test', 'test')",
                            "CREATE TABLE mongo (id Int32, name String) ENGINE=MongoDB('mongo:27017', 'db', 'table', 'user', 'pwd')",
                            "CREATE TABLE hdfs (id Int32, name String) ENGINE=HDFS('hdfs://hdfs:9000/other_storage', 'TSV')",
                            "CREATE TABLE s3 (id Int32, name String) ENGINE=S3('https://storage.yandexcloud.net/my-test-bucket-768/test-data.csv.gz', 'CSV', 'gzip')",
                            "CREATE TABLE embrdb (id Int32, name String) ENGINE = EmbeddedRocksDB PRIMARY KEY id",
                            "CREATE TABLE mysql (id Int32, name String) ENGINE = MySQL('localhost:3306', 'test', 'test', 'bayonet', '123')",
                            "CREATE TABLE postgres (id Int32, name String) ENGINE = PostgreSQL('postgres', 'test', 'test', 'postgres', 'qwerty')",
                            "CREATE TABLE ext_dist (id Int32, name String) ENGINE = ExternalDistributed('PostgreSQL', 'postgres', 'mydb', 'my_table', 'test', 'qwerty')",

                            "CREATE DATABASE sqlite_db ENGINE = SQLite('sqlite.db')",
                            "CREATE TABLE sqlite AS sqlite_db.sample Engine=MergeTree() ORDER BY id",
                            "DROP DATABASE sqlite_db",

                            "CREATE TABLE rabbitmq (id Int32, name String) ENGINE = RabbitMQ SETTINGS "
                            "rabbitmq_host_port = 'rabbitmq:5672', rabbitmq_exchange_name = 'exchange1', rabbitmq_format = 'JSONEachRow'",
                            ]

    try:
        with Given("I create tables with unsupported engines"):
            for query in create_table_queries:
                clickhouse.query(query)

        with And("I gather their schemas"):
            for table_name in table_names:
                table_schemas[table_name] = clickhouse.query(f"DESCRIBE {table_name}").output

        with And("I create MergeTree table"):
            create_and_populate_table(node=clickhouse, table_name=name_prefix, columns=self.context.columns, native=True)
            table_data = clickhouse.query(f"SELECT * FROM {name_prefix}").output

        with When("I create a backup"):
            backup.cmd(f"clickhouse-backup create {name_prefix}")

        with And("I drop all tables"):
            for table_name in table_names:
                drop_table(node=clickhouse, table_name=table_name)

        with Then("I restore tables"):
            backup.cmd(f"clickhouse-backup restore {name_prefix}")

        with And("I expect tables restored"):
            tables = clickhouse.query("SHOW TABLES").output

            with By("I expect MergeTree fully restored"):
                assert table_data == clickhouse.query(f"SELECT * FROM {name_prefix}").output, error()

            with And("I expect other tables schema restored"):
                for table_name in table_names:
                    with By(f"I check {table_name}"):
                        assert f"{table_name}" in tables, error()
                        assert clickhouse.query(f"DESCRIBE {table_name}").output == table_schemas[table_name], error()

    finally:
        with Finally("I cleanup tables"):
            drop_table(node=clickhouse, table_name=name_prefix)

            for table_name in table_names:
                drop_table(node=clickhouse, table_name=table_name)

            drop_table(node=clickhouse, table_name="dict")
            clickhouse.query("DROP DICTIONARY ndict SYNC")
            drop_table(node=clickhouse, table_name="table_for_dict")

        with And("I remove backup"):
            backup.cmd(f"clickhouse-backup delete local {name_prefix}")


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_TableEngines_OtherEngines_Kafka("1.0")
)
def kafka_engine(self):
    """Test that kafka schema can be backed up and restored.
    """
    clickhouse = self.context.nodes[0]
    backup = self.context.backup
    name_prefix = "bp_kafka"
    cluster = self.context.cluster
    topic = "dummytopic"
    consumer_group = "dummytopic_consumer_group"
    bootstrap_server = "kafka:9092"
    replication_factor = "1"
    partitions = "12"
    counts = 240000

    try:
        with Given("I Create kafka topic"):
            create_topic(kafka_node=self.context.kafka, bootstrap_server=bootstrap_server, topic=topic,
                         consumer_group=consumer_group, partitions=partitions, replication_factor=replication_factor)

        with And("I create data source table"):
            clickhouse.query(f"CREATE TABLE source_table ENGINE = Log AS SELECT toUInt32(number) as id from numbers({counts})")

        with And("I copy table data to topic"):
            command = (f"{cluster.docker_compose} exec -T clickhouse1 clickhouse client "
                       "--query='SELECT * FROM source_table FORMAT JSONEachRow' | "
                       f"{cluster.docker_compose} exec -T kafka kafka-console-producer "
                       f"--broker-list kafka:9092 --topic {topic} > /dev/null")
            cluster.command(None, command, exitcode=None)

        with And("I create destination tables pipeline"):
            sql = f"""
                CREATE TABLE {name_prefix} (
                    id UInt32
                ) ENGINE = Kafka('kafka:9092', '{topic}', '{consumer_group}', 'JSONEachRow');
                """
            clickhouse.query(sql)

        with When("I create a backup"):
            backup.cmd(f"clickhouse-backup create --tables=default.{name_prefix} {name_prefix}")

        with And("I drop table"):
            drop_table(node=clickhouse, table_name=name_prefix)

        with Then("I restore tables"):
            backup.cmd(f"clickhouse-backup restore --tables=default.{name_prefix} {name_prefix}")

        with And("I expect table restored"):
            r = clickhouse.query("SHOW TABLES").output
            assert f"{name_prefix}" in r, error()

    finally:
        with Finally("I bring up docker-compose to restore all services"):
            command = f'{cluster.docker_compose} up -d --no-recreate 2>&1 | tee'
            cluster.command(None, command)

        with And("I cleanup tables"):
            sql = f"""
                DROP TABLE IF EXISTS source_table SYNC;
                DROP TABLE IF EXISTS {name_prefix} SYNC;
                """
            clickhouse.query(sql)


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_DatabaseEngines_MaterializedMySQL("1.0"),
)
def materializedmysql(self):
    """Test that a MaterializedMySQL can be backed up and restored.
    """
    clickhouse = self.context.nodes[0]
    backup = self.context.backup
    mysql = self.context.mysql
    backup_name = "backup_mysql"

    try:
        with Given("I create database and table in MySQL"):
            mysql.cmd(f"mysql -uroot -pqwerty -e \"CREATE DATABASE mydb\"")
            mysql.cmd(f"mysql -uroot -pqwerty -e \"CREATE TABLE mydb.MyTable (id INT PRIMARY KEY, name VARCHAR(10))\"")
            for i in range(10):
                mysql.cmd(f"mysql -uroot -pqwerty -e \"INSERT INTO mydb.MyTable VALUES ({i}, '{''.join(random.choices(string.ascii_uppercase + string.digits, k=10))}')\"")

        with And("I create MaterializedMySQL"):
            clickhouse.query(f"CREATE DATABASE mysql ENGINE = MaterializedMySQL('mysql:3306', 'mydb', 'root', 'qwerty') "
                             f"SETTINGS allows_query_when_mysql_lost=true, max_wait_time_when_mysql_unavailable=10000;")
            table_contents = clickhouse.query("SELECT * FROM mysql.MyTable").output.split('\n')[:-1]

        with When(f"I create backup"):
            backup.cmd(f"clickhouse-backup create --tables=mysql.MyTable {backup_name}")

        with And("I modify original table"):
            mysql.cmd(f"mysql -uroot -pqwerty -e \"DELETE FROM mydb.MyTable WHERE id<3;\"")

        with And("I restore from backup"):
            backup.cmd(f"clickhouse-backup restore --tables=mysql.MyTable {backup_name}")
            time.sleep(5)

        with Then("expect table restored"):
            r = clickhouse.query("SHOW TABLES from mysql").output
            assert "MyTable" in r, error()

            restored = clickhouse.query("SELECT * FROM mysql.MyTable").output.split('\n')
            for row in table_contents:
                assert row in restored, error()

    finally:
        with Finally("I remove backup"):
            backup.cmd(f"clickhouse-backup delete local {backup_name}")

        with And("I drop database"):
            clickhouse.query("DROP DATABASE IF EXISTS mysql SYNC")


@TestScenario
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_DatabaseEngines_MaterializedPostgreSQL("1.0"),
)
def materializedpostgresql(self):
    """Test that a MaterializedPostgreSQL can be backed up and restored.
    """
    xfail("skipping this test for now")
    clickhouse = self.context.nodes[0]
    backup = self.context.backup
    postgres = self.context.postgres
    backup_name = "backup_psql"

    try:
        with Given("I create database and table in MySQL"):
            postgres.cmd(f"psql -Utest -c \"CREATE DATABASE mydb;\"")
            postgres.cmd(f"psql -Utest --dbname=mydb -c \"CREATE TABLE MyTable (id INTEGER PRIMARY KEY, name VARCHAR(10));\"")
            for i in range(10):
                postgres.cmd(f"psql -Utest --dbname=mydb -c \"INSERT INTO MyTable (id, name) VALUES ({i}, '{''.join(random.choices(string.ascii_uppercase + string.digits, k=10))}');\"")
            debug(postgres.cmd(f"psql -Utest --dbname=mydb -c \"select * from MyTable\"").output)
            time.sleep(10)

        with And("I create MaterializedPostgreSQL"):
            clickhouse.query(f"CREATE DATABASE psql ENGINE = MaterializedPostgreSQL('postgres:5432', 'mydb', 'test', 'qwerty')")
            debug(clickhouse.query("SHOW TABLES FROM psql").output)
            debug(backup.cmd("clickhouse-backup tables").output)
            table_contents = clickhouse.query("SELECT * FROM psql.mytable").output.split('\n')[:-1]


        with When(f"I create backup"):
            backup.cmd(f"clickhouse-backup create --tables=psql.mytable {backup_name}")

        with And("I modify original table"):
            postgres.cmd(f"psql -Utest --dbname=mydb -c \"DELETE FROM mytable WHERE id<3;\"")
            drop_table(node=clickhouse, database="psql", table_name="mytable")

        with And("restore table"):
            backup.cmd(f"clickhouse-backup restore --tables=psql.mytable {backup_name}")

        with Then("expect table restored"):
            r = clickhouse.query("SHOW TABLES FROM psql").output
            assert "mytable" in r, error()

            restored = clickhouse.query("SELECT * FROM psql.mytable").output.split('\n')
            for row in table_contents:
                assert row in restored, error()

    finally:
        with Finally("I remove backup"):
            backup.cmd(f"clickhouse-backup delete local {backup_name}")

        with And("I drop database"):
            clickhouse.query("DROP DATABASE IF EXISTS psql")


@TestFeature
@Requirements(
    RQ_SRS_013_ClickHouse_BackupUtility_TableEngines_OtherEngines("1.0")
)
def other_engines(self):
    """Check that schemas for other engines that MergeTree are backed up and those tables don't break the backup process.
    """
    for scenario in loads(current_module(), Scenario, Suite):
        Scenario(run=scenario)
