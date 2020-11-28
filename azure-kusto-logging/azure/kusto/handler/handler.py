import logging
import pandas
from azure.kusto.ingest import KustoIngestClient, IngestionProperties, FileDescriptor, BlobDescriptor, DataFormat, KustoStreamingIngestClient


class KustoHandler(logging.Handler):
    """
    A handler class which writes formatted logging records to Kusto.
    """
    def __init__(self, kcsb, database, table, data_format=DataFormat.CSV, useStreaming=False):
        """
        Initialize the appropriate kusto clienrt.
        """
        logging.Handler.__init__(self)

        # ugly workaround to avoid the libraries to call back the logger (infinite recursion)
        # need to fix
        logging.getLogger("azure").propagate = False
        logging.getLogger("oauthlib").propagate = False
        logging.getLogger("msrest").propagate = False
        logging.getLogger("msal").propagate = False
        logging.getLogger("msal_extensions").propagate = False
        logging.getLogger("asyncio").propagate = False
        logging.getLogger("concurrent").propagate = False
        logging.getLogger("adal-python").propagate = False
        logging.getLogger("requests").propagate = False
        logging.getLogger("urllib3").propagate = False

        if useStreaming:
            self.client = KustoStreamingIngestClient(kcsb)
        else:
            self.client = KustoIngestClient(kcsb)

        self.ingestion_properties =IngestionProperties(database, table, data_format=data_format)
        self.fields = None
        self.rows = []
        

    def emit(self, record):
        """
        Emit a record.
        Simply add the record in the records list
        """
        if not self.fields:
            self.fields = list(record.__dict__.keys())
        
        self.rows.append(list(record.__dict__.values()))

    def flush(self):
        """
        Flush the records in Kusto
        """
        if self.rows:
            df = pandas.DataFrame(data=self.rows, columns=self.fields)
            self.client.ingest_from_dataframe(df, self.ingestion_properties)
            self.rows.clear()

    def __repr__(self):
        level = logging.Handler.getLevelName(self.level)
        return '<%s %s (%s)>' % (self.__class__.__name__, self.baseFilename, level)
