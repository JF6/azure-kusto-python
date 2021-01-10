"""Test log adapter use
"""
import os
import sys
import logging
import time
import random
import pandas
import pytest

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.ingest import KustoStreamingIngestClient, IngestionProperties, DataFormat
from azure.kusto.logging import (
    KustoHandler,
)

from test_setup import BaseTestKustoLogging


class TestKustoHandlerLogging(BaseTestKustoLogging):
    @classmethod
    def setup_class(cls):
        super().setup_class()
        if cls.is_live_testing_ready == False:
            pytest.skip("No backend end available", allow_module_level=True)
        cls.kh = KustoHandler(kcsb=cls.kcsb, database=cls.test_db, table=cls.test_table, useStreaming=True)
        cls.kh.setLevel(logging.DEBUG)
        logger = logging.getLogger()
        logger.addHandler(cls.kh)
        cls.adapter = logging.LoggerAdapter(logger, {'connid': "::1"})
        cls.adapter.setLevel(logging.DEBUG)

    def teardown_class(cls):
        pass
        logging.getLogger().removeHandler(cls.kh)
        #super().teardown_class()

    def test_info_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        nb_of_tests = 3
        for i in range(0, nb_of_tests):
            self.adapter.info("Test {} info {}".format(__file__, i))
        self.kh.flush()
        self.assert_rows_added(nb_of_tests, logging.INFO)

    def test_debug_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        nb_of_tests = 4
        for i in range(0, nb_of_tests):
            self.adapter.debug("Test debug {}".format(i))
        self.kh.flush()
        self.assert_rows_added(nb_of_tests, logging.DEBUG)

    def test_error_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        nb_of_tests = 2
        for i in range(0, nb_of_tests):
            self.adapter.error("Test error {}".format(i))
        self.kh.flush()
        self.assert_rows_added(nb_of_tests, logging.ERROR)

    def test_critical_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        nb_of_tests = 1
        for i in range(0, nb_of_tests):
            self.adapter.critical("Test critical {}".format(i))
        self.kh.flush()
        self.assert_rows_added(nb_of_tests, logging.CRITICAL)
