#
# This Source Code Form is subject to the terms of the Mozilla Public License, v.
# 2.0 with a Healthcare Disclaimer.
# A copy of the Mozilla Public License, v. 2.0 with the Healthcare Disclaimer can
# be found under the top level directory, named LICENSE.
# If a copy of the MPL was not distributed with this file, You can obtain one at
# http://mozilla.org/MPL/2.0/.
# If a copy of the Healthcare Disclaimer was not distributed with this file, You
# can obtain one at the project website https://github.com/igia.
#
# Copyright (C) 2021-2022 Persistent Systems, Inc.
#
"""
:mod:`py_bcp` -- bcp tooling
============================
.. module:: py_bcp
    :platform: Linux/Windows
    :synopsis: module contains class for supporting bcp tool operations, wrapper on the bcp tool
"""
# __since__ = "2020-05-07"


import subprocess
import os
from i2b2_cdi.log import cdi_logging
from i2b2_cdi.database.cdi_database_connections import I2b2demoDataSource
from Mozilla.exception.cdi_bcp_failed_error import BcpUploadFailedError

logger = cdi_logging.get_logger(__file__)


class PyBCP:
    """Class provides the wrapper method on bcp tool"""

    def __init__(
            self,
            table_name,
            import_file,
            delimiter,
            batch_size,
            error_file):
        self.table_name = table_name
        self.import_file = import_file
        self.delimiter = delimiter
        self.batch_size = batch_size
        self.error_file = error_file

    def upload(self):
        """Wrapper ethod for uploading data file using bcp tool"""
        try:
            dbparams = I2b2demoDataSource()
            with dbparams:
                user = dbparams.username
                password = dbparams.password
                database = dbparams.database
                server = dbparams.server
            process = subprocess.Popen(["bcp",
                                        self.table_name,
                                        "in",
                                        self.import_file,
                                        "-U",
                                        user,
                                        "-P",
                                        password,
                                        "-d",
                                        database,
                                        "-S",
                                        server,
                                        "-c",
                                        "-t",
                                        self.delimiter,
                                        "-m",
                                        str(self.batch_size),
                                        "-e",
                                        self.error_file],
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       universal_newlines=True)
            output, errors = process.communicate()
            logger.info(output) if output else ""
            logger.error(errors) if errors else ""

            if "BCP copy in failed" in output or errors:
                raise BcpUploadFailedError("BCP copy failed")
        except Exception as e:
            logger.error(cdi_logging.format_error_log("BCP upload failed", e))
            raise

    def execute_sql(self, file_path):
        """Wrapper method to execute the queries using sqlcmd command
        Args:
            file_path (str): path to the sql script file
        """
        try:
            dbparams = I2b2demoDataSource()
            with dbparams:
                user = dbparams.username
                password = dbparams.password
                database = dbparams.database
                server = dbparams.server
            process = subprocess.Popen(["sqlcmd",
                                        "-U",
                                        user,
                                        "-P",
                                        password,
                                        "-d",
                                        database,
                                        "-S",
                                        server,
                                        "-i",
                                        file_path],
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       universal_newlines=True)
            output, errors = process.communicate()
            logger.info(output) if output else ""
            logger.error(errors) if errors else ""
            if "duplicate key" in output or errors:
                raise Exception("Sqlcmd : cannot insert duplicate key")
        except Exception as e:
            logger.error(
                cdi_logging.format_error_log(
                    "sqlcmd failed to execute the sql - " +
                    str(file_path),
                    e))
            raise


if __name__ == "__main__":
    bcp_test = PyBCP(
        table_name="observation_fact_numbered",
        import_file="demo/data/csv/deid/bcp/observation_fact.bcp",
        delimiter="/#/",
        batch_size=10000,
        error_file="err.log")
    bcp_test.execute_sql("sql/create_observation_fact_numbered.sql")
    bcp_test.upload()
    bcp_test.execute_sql("sql/load_observation_fact_from_numbered.sql")
