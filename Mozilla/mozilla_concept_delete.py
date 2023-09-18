#
# Copyright (c) 2020-2021 Massachusetts General Hospital. All rights reserved. 
# This program and the accompanying materials  are made available under the terms 
# of the Mozilla Public License v. 2.0 ( http://mozilla.org/MPL/2.0/) and under 
# the terms of the Healthcare Disclaimer.
#

from i2b2_cdi.database.cdi_database_connections import  I2b2metaDataSource
from Mozilla.exception.mozilla_cdi_database_error import CdiDatabaseError
from loguru import logger
from Mozilla.exception.mozilla_cdi_database_error import CdiDatabaseError
from i2b2_cdi.log import logger


def delete_concepts_i2b2_metadata(config):
    """Delete the metadata for the concepts from i2b2 instance"""
    try:
        logger.debug('Deleting data from i2b2 metadata and table_access')
        queries = ['truncate table i2b2', 'truncate table table_access']

        with I2b2metaDataSource(config) as cursor:
            delete(cursor, queries)
    except Exception as e:
        raise CdiDatabaseError("Couldn't delete data: {0}".format(str(e)))


def delete(cursor, queries):
    """Execute the provided query using the database cursor

    Args:
        cursor (:obj:`pyodbc.Connection.cursor`, mandatory): Cursor obtained from the Connection object connected to the database
        queries (:obj:`list of str`, mandatory): List of delete queries to be executed 
        
    """
    try:
        for query in queries:
            cursor.execute(query)
    except Exception as e:
        raise CdiDatabaseError("Couldn't delete data: {}".format(str(e)))        