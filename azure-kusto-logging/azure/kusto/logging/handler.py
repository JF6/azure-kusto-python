"""
Kusto Logging Handler
"""
import logging
import pandas

from azure.kusto.ingest import DataFormat


class KustoHandler(logging.handlers.MemoryHandler):
    """
    A handler class which writes un-formatted logging records to Kusto.
    """

    def __init__(
        self,
        kcsb,
        database,
        table,
        data_format=DataFormat.CSV,
        useStreaming=False,
        capacity=8192,
        flushLevel=logging.ERROR,
    ):
        """
        Initialize the appropriate kusto clienrt.
        """
        super().__init__(capacity, flushLevel=flushLevel)
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
        # self.rows = []
        self.first_record = None

    def emit(self, record):
        """
        Emit a record.
        Simply add the record in the records list
        """
        # print(record)
        # if len(record.__dict__.keys()) > len(self.field):
        #     self.fields = list(record.__dict__.keys())

        if not self.buffer:
            self.first_record = record  # in case of error in flush, dump the first record.
        super().emit(record)
        # self.buffer.append(record.__dict__)
        # if self.shouldFlush(record.__dict)

    def flush(self):
        """
        Flush the records in Kusto
        """
        if self.buffer:
            self.acquire()
            log_dict = [x.__dict__ for x in self.buffer]
            records_to_write = pandas.DataFrame.from_dict(log_dict, orient="columns")

            try:
                self.client.ingest_from_dataframe(records_to_write, self.ingestion_properties)
            except Exception:
                logging.Handler.handleError(self, self.first_record)
            finally:
                self.first_record = None
                self.buffer.clear()
                self.release()
