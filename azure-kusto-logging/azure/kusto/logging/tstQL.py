import os
import sys
import logging
import time
import random
import pandas
import pytest
import threading



from logging.handlers import QueueHandler, QueueListener
from queue import Queue


from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.ingest import KustoStreamingIngestClient, IngestionProperties, DataFormat
from azure.kusto.logging import (
    KustoHandler,
)


def do_logging(numberOfMessages):
    nb_of_tests = numberOfMessages
    for i in range(nb_of_tests):
        logging.info("Test_MTLR3 {} info {} from thread {}".format(__file__, i, threading.get_ident()))


class TestKustoHandlerLogging():
    def setup_class(cls):
        use_streaming_endpoint =  False
        if use_streaming_endpoint:
            engine_cs = os.environ.get("ENGINE_CONNECTION_STRING")
        else:
            engine_cs = 'https://'+'ingest-'+os.environ['ENGINE_CONNECTION_STRING'].split('https://')[-1]
        app_id = os.environ.get("APP_ID")
        app_key = os.environ.get("APP_KEY")
        auth_id = os.environ.get("AUTH_ID")
        cls.kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(engine_cs, app_id, app_key, auth_id)

        cls.test_db = os.environ.get("TEST_DATABASE")

        cls.client = KustoClient(cls.kcsb)

        cls.test_table = "logs2"

        queue_cap = 500000
        cls.kh = KustoHandler(kcsb=cls.kcsb, database=cls.test_db, table=cls.test_table, useStreaming=use_streaming_endpoint, capacity=queue_cap, flushLevel=logging.CRITICAL)
        cls.kh.setLevel(logging.DEBUG)

        cls.q = Queue()
        cls.qh = QueueHandler(cls.q)

        cls.ql = QueueListener(cls.q, cls.kh)
        cls.ql.start()

        logger = logging.getLogger()
        logger.addHandler(cls.qh)
        logger.setLevel(logging.DEBUG)



    def test_info_logging(self):
        nb_of_tests = 30000000
        for i in range(0, nb_of_tests):
            logging.info("Test5 {} info {}".format(__file__, i))

        while not self.q.empty():
            print('.', end='')
            time.sleep(1)

    def test_mt_info_logging(self):
        logging_threads = []
        expected_results = 0
        for i in range(16):
            nb_of_logging = i * 1000000
            x = threading.Thread(target=do_logging, args=(nb_of_logging,))
            x.start()
            expected_results += nb_of_logging
            logging_threads.append(x)
        for t in logging_threads:
            t.join()

        while not self.q.empty():
            print('.', end='')
            time.sleep(1)


if __name__ == "__main__":
    x = TestKustoHandlerLogging()
    x.setup_class()
    x.test_mt_info_logging()
#    x.test_info_logging()