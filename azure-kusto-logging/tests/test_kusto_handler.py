# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import json
import os
import unittest
from datetime import datetime, timedelta
import logging
import pytest

from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data import KustoClient, ClientRequestProperties
from azure.kusto.data.response import WellKnownDataSet
from dateutil.tz import UTC
from mock import patch
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.logging import KustoHandler


def mocked_client_execute(*args, **kwargs):
    class MockResponse:
        """Mock class for KustoResponse."""

        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.text = str(json_data)
            self.status_code = status_code
            self.headers = None

        def json(self):
            """Get json data from response."""
            return self.json_data

    if args[0] == "https://somecluster.kusto.windows.net/v2/rest/query":
        return MockResponse(json.loads(data), 200)

    elif args[0] == "https://somecluster.kusto.windows.net/v1/rest/mgmt":
        return MockResponse(json.loads(data), 200)

    return MockResponse(None, 404)


class KustoHandlerTests(unittest.TestCase):
    """Tests class for KustoHandler."""

    @classmethod
    def setup_class(cls):
        cls.kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication("https://somecluster.kusto.windows.net", "a", "b", "c")
        cls.kh = KustoHandler(kcsb=cls.kcsb, database="tst", table="tbl", useStreaming=True, capacity=8192)
        logging.getLogger().addHandler(cls.kh)
        logging.getLogger().setLevel(logging.INFO)

    @classmethod
    def teardown_class(cls):
        logging.getLogger().removeHandler(cls.kh)

    # @patch("requests.Session.post", side_effect=mocked_requests_post)
    @patch("azure.kusto.data.KustoClient._execute", side_effect=mocked_client_execute)
    def test_info_logging(self, mock_execute):
        prev_rows = len(self.kh.buffer)
        logging.info("Test1")
        assert len(self.kh.buffer) == prev_rows + 1

    @patch("azure.kusto.data.KustoClient._execute", side_effect=mocked_client_execute)
    def test_info_logging_again(self, mock_execute):
        info_msg = "Test2"
        prev_rows = len(self.kh.buffer)
        logging.info(info_msg)
        assert len(self.kh.buffer) == prev_rows + 1
        assert __name__ in self.kh.buffer[-1].__dict__["filename"]
        assert info_msg == self.kh.buffer[-1].__dict__["message"]

    @patch("azure.kusto.data.KustoClient._execute", side_effect=mocked_client_execute)
    def test_debug_logging(self, mock_execute):
        prev_rows = len(self.kh.buffer)
        logging.debug("Test3")  # Won't appear
        assert len(self.kh.buffer) == prev_rows

    @patch("azure.kusto.data.KustoClient._execute", side_effect=mocked_client_execute)
    def test_flush(self, mock_execute):
        self.kh.flush()
        assert len(self.kh.buffer) == 0
