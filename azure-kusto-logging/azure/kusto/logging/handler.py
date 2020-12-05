import logging
import pandas
import sys

from azure.kusto.ingest import DataFormat

class KustoHandler(logging.Handler):
    """
    A handler class which writes formatted logging records to Kusto.
    """

    def __init__(self, kcsb, database, table, data_format=DataFormat.CSV, useStreaming=False):
        """
        Initialize the appropriate kusto clienrt.
        """
        logging.Handler.__init__(self)
        from azure.kusto.ingest import KustoIngestClient, IngestionProperties, KustoStreamingIngestClient

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

    def emit(self, record):
        """
        Emit a record.
        Simply add the record in the records list
        """
        #print(record)
        # if len(record.__dict__.keys()) > len(self.field):
        #     self.fields = list(record.__dict__.keys())

        if not self.rows:
            self.first_record = record      # in case of error in flush, dump the first record.
        self.rows.append(record.__dict__)

    def flush(self):
        """
        Flush the records in Kusto
        """
        if self.rows:
            #df = pandas.DataFrame(data=self.rows, columns=self.fields)
            df = pandas.DataFrame.from_dict(self.rows, orient='columns')
            
            #print(df.head(5))
            try:
                self.client.ingest_from_dataframe(df, self.ingestion_properties)
            except Exception as ex:
                logging.Handler.handleError(self, self.first_record)
                # print("Error kusto logging -> {}".format(ex), file=sys.stderr)
                # print(df, file=sys.stderr)
                # print("", file=sys.stderr)
            finally:
                self.first_record = None
                self.rows.clear()

    def __repr__(self):
        level = logging.Handler.getLevelName(self.level)
        return "<%s %s (%s)>" % (self.__class__.__name__, self.baseFilename, level)
