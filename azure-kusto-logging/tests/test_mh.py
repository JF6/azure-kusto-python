import os
import sys
import logging
import time
import random
import pandas

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.ingest import KustoStreamingIngestClient, IngestionProperties, DataFormat
from azure.kusto.logging import (
    KustoHandler,
    FlushableMemoryHandler,
)

from test_setup import BaseTestKustoLogging


class TestKustoMemoryHandlerLogging(BaseTestKustoLogging):
    @classmethod
    def setup_class(cls):
        super().setup_class()
        cls.kh = KustoHandler(kcsb=cls.kcsb, database=cls.test_db, table=cls.test_table, useStreaming=True)
        cls.kh.setLevel(logging.DEBUG)
        cls.memoryhandler = FlushableMemoryHandler(
            capacity=8192,
            flushLevel=logging.ERROR,
            target=cls.kh,
            flushTarget=True,
            flushOnClose=True
        )
        cls.memoryhandler.setLevel(logging.DEBUG)
        logger = logging.getLogger()
        logger.addHandler(cls.memoryhandler)
        logger.setLevel(logging.DEBUG)
        
    def test_info_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        nb_of_tests = 3
        for i in range(0,nb_of_tests):
            logging.info("Test info {}".format(i))
        self.memoryhandler.flush()
        self.assert_rows_added(nb_of_tests, logging.INFO)

    def test_debug_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        nb_of_tests = 4
        for i in range(0,nb_of_tests):
            logging.debug("Test debug {}".format(i))
        self.memoryhandler.flush()
        self.assert_rows_added(nb_of_tests, logging.DEBUG)

    def test_error_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        nb_of_tests = 2
        for i in range(0,nb_of_tests):
            logging.error("Test error {}".format(i))
        self.memoryhandler.flush()
        self.assert_rows_added(nb_of_tests, logging.ERROR)

    def test_critical_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        nb_of_tests = 1
        for i in range(0,nb_of_tests):
            logging.critical("Test critical {}".format(i))
        self.memoryhandler.flush()
        self.assert_rows_added(nb_of_tests, logging.CRITICAL)
