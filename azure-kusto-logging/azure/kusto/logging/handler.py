"""
Kusto Logging Handler
"""
import logging
import pandas

from azure.kusto.ingest import DataFormat


class KustoHandler(logging.Handler):
    """
    A handler class which writes un-formatted logging records to Kusto.
    """

    def __init__(self, kcsb, database, table, data_format=DataFormat.CSV, useStreaming=False):
        """
        Initialize the appropriate kusto clienrt.
        """
        logging.Handler.__init__(self)
        from azure.kusto.ingest import (
            KustoIngestClient,
            IngestionProperties,
            KustoStreamingIngestClient,
        )

        logging.getLogger("azure").propagate = False
        # logging.getLogger("oauthlib").propagate = False
        # logging.getLogger("msrest").propagate = False
        # logging.getLogger("msal").propagate = False
        # logging.getLogger("msal_extensions").propagate = False
        # logging.getLogger("asyncio").propagate = False
        # logging.getLogger("concurrent").propagate = False
        logging.getLogger("adal-python").propagate = False
        logging.getLogger("requests").propagate = False
        logging.getLogger("urllib3").propagate = False

        if useStreaming:
            self.client = KustoStreamingIngestClient(kcsb)
        else:
            self.client = KustoIngestClient(kcsb)

        self.ingestion_properties = IngestionProperties(database, table, data_format=data_format)
        self.rows = []
        self.first_record = None

    def emit(self, record):
        """
        Emit a record.
        Simply add the record in the records list
        """
        # print(record)
        # if len(record.__dict__.keys()) > len(self.field):
        #     self.fields = list(record.__dict__.keys())

        if not self.rows:
            self.first_record = record  # in case of error in flush, dump the first record.
        self.rows.append(record.__dict__)

    def flush(self):
        """
        Flush the records in Kusto
        """
        if self.rows:
            records_to_write = pandas.DataFrame.from_dict(self.rows, orient="columns")

            # print(df.head(5))
            try:
                self.client.ingest_from_dataframe(records_to_write, self.ingestion_properties)
            except Exception:
                logging.Handler.handleError(self, self.first_record)
            finally:
                self.first_record = None
                self.rows.clear()
