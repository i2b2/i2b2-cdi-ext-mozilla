#
# Copyright (c) 2020-2021 Massachusetts General Hospital. All rights reserved. 
# This program and the accompanying materials  are made available under the terms 
# of the Mozilla Public License v. 2.0 ( http://mozilla.org/MPL/2.0/) and under 
# the terms of the Healthcare Disclaimer.
#


from i2b2_cdi.encounter import deid_encounter as DeidEncounter
from i2b2_cdi.encounter import transform_file as TransformFile
from i2b2_cdi.log import logger
from i2b2_cdi.common.utils import *
from i2b2_cdi.common.constants import *


def load_encounters(config,encounter_files):
    """Load encounters from the given file to the i2b2 instance using bcp tool.
    Args:
        encounter_files (:obj:`str`, mandatory):List of files which needs to be imported
    """
    try:
        from perform_encounter import create_encounter_mapping, bcp_upload_encounters
        # Create encounter mapping
        create_encounter_mapping(encounter_files,config)
        for file_path in encounter_files:
            extractFileName = file_path.split("/")
            extractFileName = extractFileName[-1]
            deid_file_path, error_file_path = DeidEncounter.do_deidentify(file_path,config)
            logger.info("Check error logs of encounter de-identification if any : {}", error_file_path)
            bcp_file_path, error_file_path = TransformFile.do_transform(deid_file_path,config)
            logger.info("Check error logs of csv to bcp conversion if any : {}", error_file_path)

            bcp_upload_encounters(bcp_file_path,config)
        logger.success(SUCCESS)
    except Exception as e:
        logger.error("Failed to load encounters : {}", e)
        raise