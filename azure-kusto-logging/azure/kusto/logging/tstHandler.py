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



class TestKustoHandlerLogging():
    def setup_class(cls):
        engine_cs = os.environ.get("ENGINE_CONNECTION_STRING")
        app_id = os.environ.get("APP_ID")
        app_key = os.environ.get("APP_KEY")
        auth_id = os.environ.get("AUTH_ID")
        cls.kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(engine_cs, app_id, app_key, auth_id)

        cls.test_db = os.environ.get("TEST_DATABASE")

        cls.client = KustoClient(cls.kcsb)

        cls.test_table = "logs2"

        cls.kh = KustoHandler(kcsb=cls.kcsb, database=cls.test_db, table=cls.test_table, useStreaming=True)
        cls.kh.setLevel(logging.DEBUG)
        logger = logging.getLogger()
        logger.addHandler(cls.kh)
        cls.adapter = logging.LoggerAdapter(logger, {'extra111': "Manjaro112", "ex":"zorg"})
        cls.adapter.setLevel(logging.DEBUG)


    def test_info_logging(self):
        nb_of_tests = 3
        for i in range(0, nb_of_tests):
            self.adapter.info("Test {} info {}".format(__file__, i))
        self.kh.flush()

if __name__ == "__main__":
    x = TestKustoHandlerLogging()
    x.setup_class()
    x.test_info_logging()