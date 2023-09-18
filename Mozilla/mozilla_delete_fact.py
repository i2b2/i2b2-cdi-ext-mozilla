#
# Copyright (c) 2020-2021 Massachusetts General Hospital. All rights reserved. 
# This program and the accompanying materials  are made available under the terms 
# of the Mozilla Public License v. 2.0 ( http://mozilla.org/MPL/2.0/) and under 
# the terms of the Healthcare Disclaimer.
#

from i2b2_cdi.database.cdi_database_connections import I2b2crcDataSource
from Mozilla.exception.mozilla_cdi_database_error import CdiDatabaseError
from i2b2_cdi.log import logger
from i2b2_cdi.common.constants import *

def delete_facts_i2b2_demodata(config):
    """Delete the observation facts data from i2b2 instance"""

    try:
        logger.info(
            "Deleting data from observation_fact")
        queries = ['truncate table observation_fact']

        with I2b2crcDataSource(config) as cursor:
            for query in queries:
                cursor.execute(query)
            logger.success(SUCCESS)
    except Exception as e:
        raise CdiDatabaseError("Couldn't delete data: {0}".format(str(e)))