import os
import sys
import logging
import time
import random
import pandas
import pytest
import threading

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.ingest import KustoStreamingIngestClient, IngestionProperties, DataFormat
from azure.kusto.logging import (
    KustoHandler,
)

from test_setup import BaseTestKustoLogging


def do_logging(numberOfMessages):
    nb_of_tests = numberOfMessages
    for i in range(nb_of_tests):
        logging.info("Test {} info {} from thread {}".format(__file__, i, threading.get_native_id()))

class TestKustoHandlerMultiThreadLogging(BaseTestKustoLogging):
    @classmethod
    def setup_class(cls):
        super().setup_class()
        if cls.is_live_testing_ready == False:
            pytest.skip("No backend end available", allow_module_level=True)
        cls.kh = KustoHandler(kcsb=cls.kcsb, database=cls.test_db, table=cls.test_table, useStreaming=True)
        cls.kh.setLevel(logging.DEBUG)
        logging.getLogger().addHandler(cls.kh)
        logging.getLogger().setLevel(logging.DEBUG)

    def teardown_class(cls):
        logging.getLogger().removeHandler(cls.kh)
        super().teardown_class()

    def test_info_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        logging_threads = []
        expected_results = 0
        for i in range(10):
            nb_of_logging = i*1000
            x = threading.Thread(target=do_logging, args=(nb_of_logging,))
            x.start()
            expected_results += nb_of_logging
            logging_threads.append(x)
        for t in logging_threads:
            t.join()
        self.kh.flush()
        self.assert_rows_added(expected_results, logging.INFO)

