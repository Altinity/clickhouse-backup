import random
import string
import datetime


def random_datetime(dt_start=datetime.datetime(1971, 1, 1), dt_end=datetime.datetime(2001, 1, 1)):
    delta = dt_end - dt_start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return dt_start + datetime.timedelta(seconds=random_second)


def random_string():
    q = ''.join(random.choice(string.ascii_lowercase + string.ascii_uppercase) for _ in range(15))
    return q


def create_table_query(name, num):
    q = ""
    for i in range(num):
        q += f"CREATE TABLE IF NOT EXISTS default.{name}{i} (OrderBy Int32, C2 Int8, C3 UInt8, C4 String, C5 DateTime, C6 Int8, C7 DateTime64, " \
            f"C8 DateTime64, C9 Int16, C10 UInt64, C11 String, C12 DateTime, C13 Int8, C14 DateTime64)" \
            f"Engine = MergeTree() ORDER BY OrderBy;"
    print(q)


def populate_table_query(name, num):
    q = ""
    for i in range(num):
        inserts = f"({random.randint(0, 128)}, {random.randint(0, 128)}, {random.randint(0, 128)}, " \
            f"'{random_string()}', '{random_datetime()}', {random.randint(0, 128)}, '{random_datetime()}'," \
            f"'{random_datetime()}', {random.randint(0, 128)}, {random.randint(0, 128)}, '{random_string()}', " \
            f"'{random_datetime()}', {random.randint(0, 128)}, '{random_datetime()}')"
        for _ in range(30):
            inserts += f", ({random.randint(0, 128)}, {random.randint(0, 128)}, {random.randint(0, 128)}, " \
            f"'{random_string()}', '{random_datetime()}', {random.randint(0, 128)}, '{random_datetime()}'," \
            f"'{random_datetime()}', {random.randint(0, 128)}, {random.randint(0, 128)}, '{random_string()}', " \
            f"'{random_datetime()}', {random.randint(0, 128)}, '{random_datetime()}') "

        q += f"INSERT INTO default.{name}{i} (*) VALUES {inserts}; "
    print(q)


def drop_table_query(name, num):
    q = ""
    for i in range(num):
        q += f"DROP TABLE IF EXISTS default.{name}{i} SYNC;"
    print(q)



