# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import json
import os
import unittest
from datetime import datetime, timedelta

import pytest
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data import KustoClient, ClientRequestProperties
from azure.kusto.data.response import WellKnownDataSet
from dateutil.tz import UTC
from mock import patch
import pandas
import logging
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.logging import (
    KustoHandler,
)
PANDAS = True

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

    #@patch("requests.Session.post", side_effect=mocked_requests_post)
    @patch("azure.kusto.data.KustoClient._execute", side_effect=mocked_client_execute)
    def test_info_logging(self, mock_execute):
        kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication("https://somecluster.kusto.windows.net", "a", "b", "c")
        kh = KustoHandler(kcsb=kcsb, database="tst", table="tbl", useStreaming=True)
        logging.getLogger().addHandler(kh)
        logging.getLogger().setLevel(logging.INFO)
        logging.info("Test1")
        assert len(kh.rows)==1
        info_msg = "Test2"
        logging.info(info_msg)
        assert len(kh.rows)==2
        assert __name__ in kh.rows[1]['filename']
        assert info_msg == kh.rows[1]['message']
        logging.debug("Test3")  # Won't appear
        assert len(kh.rows)==2
        kh.flush()
        assert len(kh.rows)==0


