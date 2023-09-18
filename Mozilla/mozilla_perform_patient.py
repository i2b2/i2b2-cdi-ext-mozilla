#
# Copyright (c) 2020-2021 Massachusetts General Hospital. All rights reserved. 
# This program and the accompanying materials  are made available under the terms 
# of the Mozilla Public License v. 2.0 ( http://mozilla.org/MPL/2.0/) and under 
# the terms of the Healthcare Disclaimer.
#

from i2b2_cdi.patient import patient_mapping
from i2b2_cdi.common.constants import *
from loguru import logger

def load_patient_mapping(mrn_files,factfile=None): 
    """Load patient mapping from the given mrn file to the i2b2 instance.

    Args:
        mrn_files (:obj:`str`, mandatory): Path to the files which needs to be imported
    """
    from perform_patient import bcp_upload_patient_mapping
    logger.debug("Importing patient mappings")
    rows_skipped_for_mrn = None
    try:
        for file in mrn_files:
            extractFileName = file.split("/")[-1]  
            bcp_file_path,rows_skipped_for_mrn = patient_mapping.create_patient_mapping(file,factfile)
            bcp_upload_patient_mapping(bcp_file_path)               
        logger.success(SUCCESS)
        return rows_skipped_for_mrn
    except Exception as e:
        logger.error("Failed to load patient mapping : {}", e)
        raise e