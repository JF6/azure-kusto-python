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
    FlushableMemoryHandler,
)


from test_setup import BaseTestKustoLogging


def do_logging(numberOfMessages):
    nb_of_tests = numberOfMessages
    for i in range(nb_of_tests):
        logging.warning("Test {} warning {} from thread {}".format(__file__, i, threading.get_ident()))


class TestKustoQueueListenerMemoryHandlerLogging(BaseTestKustoLogging):
    @classmethod
    def setup_class(cls):
        super().setup_class()
        if cls.is_live_testing_ready == False:
            pytest.skip("No backend end available", allow_module_level=True)

        # logging.basicConfig(filename="test_ql.log", filemode="w", format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        queue_cap = 5000
        cls.kh = KustoHandler(kcsb=cls.kcsb, database=cls.test_db, table=cls.test_table, useStreaming=True, capacity=queue_cap, flushLevel=logging.CRITICAL)
        cls.kh.setLevel(logging.DEBUG)

        cls.q = Queue(queue_cap*4)
        cls.qh = QueueHandler(cls.q)

        cls.ql = QueueListener(cls.q, cls.kh)
        cls.ql.start()

        logger = logging.getLogger()
        logger.addHandler(cls.qh)
        logger.setLevel(logging.DEBUG)


    def teardown_class(cls):
        cls.ql.stop()
        time.sleep(5)  # in order to wait before deleting the table
        logging.getLogger().removeHandler(cls.ql)
        super().teardown_class()

    def test_info_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        nb_of_tests = 30000000
        for i in range(0, nb_of_tests):
            logging.info("Test {} info {}".format(__file__, i))
        logging.critical("Flush")
        self.assert_rows_added(nb_of_tests, logging.INFO, timeout=10000)

    def test_debug_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        nb_of_tests = 40000
        for i in range(0, nb_of_tests):
            logging.debug("Test debug {}".format(i))
        logging.critical("Flush")
        self.assert_rows_added(nb_of_tests, logging.DEBUG, timeout=500)

    def test_error_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        nb_of_tests = 20000
        for i in range(0, nb_of_tests):
            logging.error("Test error {}".format(i))
        logging.critical("Flush")
        self.assert_rows_added(nb_of_tests, logging.ERROR, timeout=500)

    def test_critical_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        nb_of_tests = 20
        for i in range(0, nb_of_tests):
            logging.critical("Test critical {}".format(i))
        self.assert_rows_added(nb_of_tests, logging.CRITICAL)

    def test_mt_warning_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        logging_threads = []
        expected_results = 0
        for i in range(100):
            nb_of_logging = i * 1000
            x = threading.Thread(target=do_logging, args=(nb_of_logging,))
            x.start()
            expected_results += nb_of_logging
            logging_threads.append(x)
        for t in logging_threads:
            t.join()
        logging.critical("Flush")
        self.assert_rows_added(expected_results, logging.WARNING)
