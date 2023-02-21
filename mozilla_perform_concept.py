#
# Copyright (c) 2020-2021 Massachusetts General Hospital. All rights reserved. 
# This program and the accompanying materials  are made available under the terms 
# of the Mozilla Public License v. 2.0 ( http://mozilla.org/MPL/2.0/) and under 
# the terms of the Healthcare Disclaimer.
#

from i2b2_cdi.concept import concept_delete
from i2b2_cdi.log import logger
from i2b2_cdi.common.constants import *

def delete_concepts(config):
    """Delete the concepts from i2b2 instance"""
    logger.debug("Deleting concepts")
    try:
        concept_delete.delete_concepts_i2b2_metadata(config)
        concept_delete.delete_concepts_i2b2_demodata(config)
        logger.success(SUCCESS)
    except Exception as e:
        logger.error("Failed to delete concepts : {}", e)
        raise