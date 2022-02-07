import os
import yaml
import time
import random
import requests
import datetime

from testflows.core import *
from testflows.asserts import *


simple_data_types_columns = {
    "misc": {"OrderBy": "Int8", "Sign": "Int8", "Version": "UInt8", "Path": "String", "Time": "DateTime", "Value": "Int8"},
    "integer": {"Int8": "Int8", "Int16": "Int16", "Int32": "Int32", "Int64": "Int64", "Int128": "Int128", "Int256": "Int256"},
    "unsigned": {"UInt8": "UInt8", "UInt16": "UInt16", "UInt32": "UInt32", "UInt64": "UInt64", "UInt256": "UInt256"},
    "float": {"Float32": "Float32", "Float64": "Float64"},
    "decimal": {"Decimal32": "Decimal32(9)", "Decimal64": "Decimal64(18)", "Decimal128": "Decimal128(38)", "Decimal256": "Decimal256(76)"},
    "string": {"FixedString": "FixedString(16)"},
    "uuid": {"UUID": "UUID"},
    "date": {"Date": "Date", "Date32": "Date32", "DateTime64": "DateTime64"},
    "enum": {"Enum": "Enum(\'hello\' = 1, \'world\' = 2)"},
    "geo": {"Point": "Point", "Ring": "Ring", "Polygon": "Polygon", "MultiPolygon": "MultiPolygon"},
    "ip": {"IPv4": "IPv4", "IPv6": "IPv6"}
}


def random_datetime(dt_start, dt_end):
    delta = dt_end - dt_start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return dt_start + datetime.timedelta(seconds=random_second)


def config_modifier(fields={}):
    path = current().context.backup_config_file
        
    with open(path) as f:
        s = yaml.full_load(f)
    with open(path, 'w') as f:
        for section in fields:
            for key in fields[section]:
                s[section][key] = fields[section][key]
        yaml.dump(s, f, default_flow_style=False)


def api_request(endpoint: str, type="get", payload={}, check_status=None, wait_ready=False):
    """This helper performs an API request and checks status code if needed.
    """
    with When("perform API request"):
        r = getattr(requests, type)(f"{endpoint}", json=payload)

        if check_status:
            with Then(f"expect code {check_status}"):
                assert r.status_code == check_status, error()

        return r


def wait_request_finalized(url: str):
    """This helper waits for API request to finalize.
    """
    with Step("wait for the query to finalize"):
        r = api_request(endpoint=f"{url}/backup/status")
        while r.text and r.json()["status"] == "in progress":
            r = api_request(endpoint=f"{url}/backup/status")
            time.sleep(1)
