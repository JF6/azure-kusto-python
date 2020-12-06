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
)


class BaseTestKustoLogging:
    @classmethod
    def setup_class(cls):
        """create the Kusto table and initialize kcsb info"""
        engine_cs = os.environ.get("ENGINE_CONNECTION_STRING")
        app_id = os.environ.get("APP_ID")
        app_key = os.environ.get("APP_KEY")
        auth_id = os.environ.get("AUTH_ID")
        cls.kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(engine_cs, app_id, app_key, auth_id)
        
        cls.test_db = os.environ.get("TEST_DATABASE")
        cls.client = KustoClient(cls.kcsb)

        python_version = "_".join([str(v) for v in sys.version_info[:3]])
        cls.test_table = "python_test_{0}_{1}_{2}".format(python_version, str(int(time.time())), random.randint(1, 100000))

        with open("azure-kusto-logging/tests/createTable.kql") as f:
            tbl_create = f.read()
        cls.client.execute(cls.test_db,tbl_create.format(cls.test_table))
        #time.sleep(200) # wait for the table to be created

        timeout = 200
        csv_ingest_props = IngestionProperties(
            cls.test_db,
            cls.test_table,
            data_format=DataFormat.CSV,
            flush_immediately=True,
        )

        # Wait for the table to be able to ingest.
        streaming_ingest_client = KustoStreamingIngestClient(cls.kcsb)
        df = pandas.DataFrame.from_dict({"msg": ["Flush"]}) 
        while timeout > 0:
            time.sleep(1)
            timeout -= 1

            try:
                streaming_ingest_client.ingest_from_dataframe(df, csv_ingest_props)
                response = cls.client.execute(cls.test_db,"{}  where name == 'Flush' | count".format(cls.test_table))
            except KustoServiceError:
                continue

            if response is not None:
                row = response.primary_results[0][0]
                actual = int(row["Count"])
                # this is done to allow for data to arrive properly
                if actual >= 1:
                    break

    
    @classmethod
    def teardown_class(cls):
        #client.execute(test_db, ".drop table {} ifexists".format(test_table))
        pass

    @classmethod
    # assertions
    def assert_rows_added(cls, expected: int, level: int, timeout=60):
        current_count=0

        actual = 0
        while timeout > 0:
            time.sleep(1)
            timeout -= 1

            try:
                response = cls.client.execute(cls.test_db, "{} | where levelno=={} | where msg != 'Flush' | count".format(cls.test_table, level))
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

class TestKustoHandlerLogging(BaseTestKustoLogging):
    @classmethod
    def setup_class(cls):
        super().setup_class()
        cls.kh = KustoHandler(kcsb=cls.kcsb, database=cls.test_db, table=cls.test_table, useStreaming=True)
        cls.kh.setLevel(logging.DEBUG)
        logging.getLogger().addHandler(cls.kh)
        logging.getLogger().setLevel(logging.DEBUG)
        
    def test_info_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        nb_of_tests = 3
        for i in range(0,nb_of_tests):
            logging.info("Test info {}".format(i))
        self.kh.flush()
        self.assert_rows_added(nb_of_tests, logging.INFO)

    def test_debug_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        nb_of_tests = 4
        for i in range(0,nb_of_tests):
            logging.debug("Test debug {}".format(i))
        self.kh.flush()
        self.assert_rows_added(nb_of_tests, logging.DEBUG)

    def test_error_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        nb_of_tests = 2
        for i in range(0,nb_of_tests):
            logging.error("Test error {}".format(i))
        self.kh.flush()
        self.assert_rows_added(nb_of_tests, logging.ERROR)

    def test_critical_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        nb_of_tests = 1
        for i in range(0,nb_of_tests):
            logging.critical("Test critical {}".format(i))
        self.kh.flush()
        self.assert_rows_added(nb_of_tests, logging.CRITICAL)
