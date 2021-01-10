Microsoft Azure Kusto Logging for Python
========================================

Overview
--------

Azure kusto logging is a handler which bufferize traces then write them in Kusto. 

Logging is done through the standard python mechanism with the exception of formatters, which makes little sense in a context where the traces will be queried on the attribute values.

.. code-block:: python

    from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

    cluster = "<insert here your cluster name>"
    client_id = "<insert here your AAD application id>"
    client_secret = "<insert here your AAD application key>"
    authority_id = "<insert here your AAD tenant id>"

    kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, client_id, client_secret, authority_id)
    client = KustoClient(kcsb)



Patterns
--------

The most appropriate use is to leverage the queue handler / queue listener

.. code-block:: python

    kh = KustoHandler(kcsb=kcsb, database=test_db, table=test_table, useStreaming=True, capacity=50000, flushLevel=logging.CRITICAL)
    kh.setLevel(logging.INFO)

    q = Queue()
    qh = QueueHandler(q)

    ql = QueueListener(q, kh)
    ql.start()

    logger = logging.getLogger()
    logger.addHandler(qh)
    logger.setLevel(logging.INFO)


The handler can be used with_aad_application_key_authentication

.. code-block:: python

    kh = KustoHandler(kcsb=kcsb, database=test_db, table=test_table, useStreaming=True)
    kh.setLevel(logging.INFO)
    logging.getLogger().addHandler(kh)
    logging.getLogger().setLevel(logging.INFO)


*Kusto Python Client* Library provides the capability to query Kusto clusters using Python.
It is Python 3.x compatible and supports
all data types through familiar Python DB API interface.

It's possible to use the library, for instance, from `Jupyter Notebooks
<http://jupyter.org/>`_.
which are attached to Spark clusters,
including, but not exclusively, `Azure Databricks
<https://azure.microsoft.com/en-us/services/databricks/>`_. instances.

* `How to install the package <https://github.com/Azure/azure-kusto-python#install>`_.

* `Kusto query sample <https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-data/tests/sample.py>`_.

* `GitHub Repository <https://github.com/Azure/azure-kusto-python/tree/master/azure-kusto-data>`_.
