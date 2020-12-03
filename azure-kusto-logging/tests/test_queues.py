# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import datetime
import io
import os
import random
import sys
import time
from time import sleep
import uuid
import logging

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.logging import (
    FlushableMemoryHandler,
    KustoHandler,
)
from queue import Queue
from logging.handlers import QueueHandler, QueueListener

CLEAR_DB_CACHE = ".clear database cache streamingingestion schema"


def engine_kcsb_from_env() -> KustoConnectionStringBuilder:
    engine_cs = os.environ.get("ENGINE_CONNECTION_STRING")
    app_id = os.environ.get("APP_ID")
    app_key = os.environ.get("APP_KEY")
    auth_id = os.environ.get("AUTH_ID")
    return KustoConnectionStringBuilder.with_aad_application_key_authentication(engine_cs, app_id, app_key, auth_id)


def dm_kcsb_from_env() -> KustoConnectionStringBuilder:
    engine_cs = os.environ.get("ENGINE_CONNECTION_STRING")
    dm_cs = os.environ.get("DM_CONNECTION_STRING") or engine_cs.replace("//", "//ingest-")
    app_id = os.environ.get("APP_ID")
    app_key = os.environ.get("APP_KEY")
    auth_id = os.environ.get("AUTH_ID")
    return KustoConnectionStringBuilder.with_aad_application_key_authentication(dm_cs, app_id, app_key, auth_id)


#TODO
def teardown_module():
    global ql

    ql.stop()
    #client.execute(test_db, ".drop table {} ifexists".format(test_table))
    pass


def setup_module():
    global ql
    global client
    global test_db
    global test_table

    # Init clients
    test_db = os.environ.get("TEST_DATABASE")

    python_version = "_".join([str(v) for v in sys.version_info[:3]])
    test_table = "python_test_{0}_{1}_{2}".format(python_version, str(int(time.time())), random.randint(1, 100000))
    kcsb = engine_kcsb_from_env()
    client = KustoClient(kcsb)
    # ingest_client = KustoIngestClient(dm_kcsb_from_env())
    # streaming_ingest_client = KustoStreamingIngestClient(engine_kcsb_from_env())

    start_time = datetime.datetime.now(datetime.timezone.utc)

    with open("azure-kusto-logging/tests/createTable.kql") as f:
        tbl_create = f.read()
    client.execute(test_db,tbl_create.format(test_table))
    sleep(100) # wait for the table to be created

    current_count = 0

    kh = KustoHandler(kcsb=kcsb, database=test_db, table=test_table, useStreaming=True)

    q = Queue()
    qh = QueueHandler(q)

    memoryhandler = FlushableMemoryHandler(
        capacity=8192,
        flushLevel=logging.ERROR,
        target=kh,
        flushTarget=True,
        flushOnClose=True
    )

    ql = QueueListener(q, memoryhandler)
    ql.start()

    logger = logging.getLogger()
    logger.addHandler(qh)
    logger.setLevel(logging.DEBUG)



# assertions
def assert_rows_added(expected: int, level: int, timeout=60):
    current_count=0

    actual = 0
    while timeout > 0:
        time.sleep(1)
        timeout -= 1

        try:
            response = client.execute(test_db, "{} | where levelno=={} | where msg != 'Flush' | count".format(test_table, level))
        except KustoServiceError:
            continue

        if response is not None:
            row = response.primary_results[0][0]
            actual = int(row["Count"]) - current_count
            # this is done to allow for data to arrive properly
            if actual >= expected:
                break

    current_count += actual
    assert actual == expected, "Row count expected = {0}, while actual row count = {1}".format(expected, actual)



def test_qh_info_logging(caplog):
    caplog.set_level(logging.CRITICAL, logger="adal-python")
    caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
    for i in range(0,50):
        logging.info("Test info {0}".format(i))
    logging.error('Flush')
    assert_rows_added(50, logging.INFO)

# def test_qh_debug_logging(caplog):
#     caplog.set_level(logging.CRITICAL, logger="adal-python")
#     caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
#     for i in range(0,100):
#         logging.debug("Test debug {0}".format(i))
#     logging.error('Flush')
#     assert_rows_added(100, logging.DEBUG)

def test_qh_critical_logging(caplog):
    caplog.set_level(logging.CRITICAL, logger="adal-python")
    caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
    for i in range(0,5):
        logging.fatal("Test fatal {0}".format(i))
    assert_rows_added(5, logging.CRITICAL)

def test_qh_error_logging(caplog):
    caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
    for i in range(0,10):
        logging.error("Test error {0}".format(i))
    assert_rows_added(10, logging.ERROR)
