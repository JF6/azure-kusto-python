import os
import sys
import logging
import time
import random
import pandas
import pytest

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


class TestKustoQueueListenerMemoryHandlerLogging(BaseTestKustoLogging):
    @classmethod
    def setup_class(cls):
        super().setup_class()
        if  cls.is_live_testing_ready == False:
            pytest.skip("No backend end available", allow_module_level=True)

        kh = KustoHandler(kcsb=cls.kcsb, database=cls.test_db, table=cls.test_table, useStreaming=True)
        kh.setLevel(logging.DEBUG)

        q = Queue()
        qh = QueueHandler(q)

        memoryhandler = FlushableMemoryHandler(capacity=8192, flushLevel=logging.ERROR, target=kh, flushTarget=True, flushOnClose=True)
        memoryhandler.setLevel(logging.DEBUG)
        cls.ql = QueueListener(q, memoryhandler)
        cls.ql.start()

        logger = logging.getLogger()
        logger.addHandler(qh)
        logger.setLevel(logging.DEBUG)

    def teardown_class(cls):
        cls.ql.stop()
        time.sleep(5)  # in order to wait before deleting the table
        super().teardown_class()

    def test_info_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        nb_of_tests = 3
        for i in range(0, nb_of_tests):
            logging.info("Test info {}".format(i))
        logging.error("Flush")
        self.assert_rows_added(nb_of_tests, logging.INFO)

    def test_debug_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        nb_of_tests = 4
        for i in range(0, nb_of_tests):
            logging.debug("Test debug {}".format(i))
        logging.error("Flush")
        self.assert_rows_added(nb_of_tests, logging.DEBUG)

    def test_error_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        nb_of_tests = 2
        for i in range(0, nb_of_tests):
            logging.error("Test error {}".format(i))
        self.assert_rows_added(nb_of_tests, logging.ERROR)

    def test_critical_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        nb_of_tests = 1
        for i in range(0, nb_of_tests):
            logging.critical("Test critical {}".format(i))
        self.assert_rows_added(nb_of_tests, logging.CRITICAL)
