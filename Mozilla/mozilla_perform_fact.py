
#
# Copyright (c) 2020-2021 Massachusetts General Hospital. All rights reserved. 
# This program and the accompanying materials  are made available under the terms 
# of the Mozilla Public License v. 2.0 ( http://mozilla.org/MPL/2.0/) and under 
# the terms of the Healthcare Disclaimer.
#

import glob
from pathlib import Path
from i2b2_cdi.fact import deid_fact as DeidFact
from i2b2_cdi.fact import transform_file as TransformFile
from i2b2_cdi.log import logger
from i2b2_cdi.common.bulk_uploader import BulkUploader
from i2b2_cdi.common.constants import *
from i2b2_cdi.fact import concept_cd_map as ConceptCdMap
import os
from importlib.resources import files

def load_facts(file_list,config): 
    """Load the facts from the given file to the i2b2 instance using bcp tool.

    Args:
        file_list (:obj:`str`, mandatory): List of files from which facts to be imported

    """
    
    try:
        # Get concept_cd map for fact validation and to decide column
        concept_map = {}
        factsErrorsList = []
        if not config.disable_fact_validation:
            concept_map = ConceptCdMap.get_concept_code_mapping(config)
        for _file in file_list:
            extractFileName = _file.split("/")
            extractFileName = extractFileName[-1]
            filename="/usr/src/app/tmp/"+"log_"+extractFileName  
        
            deid_file_path, error_file_path = DeidFact.do_deidentify(_file, concept_map,config)
            logger.debug("Check error logs of fact de-identification if any : " + error_file_path)
            factsErrorsList.append(error_file_path)
            bcp_file_path = TransformFile.csv_to_bcp(deid_file_path, concept_map, config)
        bulk_uploader = BulkUploader(
                table_name='observation_fact',
                import_file=bcp_file_path,
                delimiter=str(config.bcp_delimiter),
                batch_size=10000,
                error_file="/usr/src/app/tmp/benchmark/logs/error_bcp_facts.log")
        drop_indexes = Path('i2b2_cdi/resources/sql') / \
        'drop_indexes_observation_fact_pg.sql'

        if not os.path.exists(drop_indexes):
            drop_indexes = files('i2b2_cdi.resources.sql').joinpath('drop_indexes_observation_fact_pg.sql')

        if(str(config.crc_db_type)=='pg'):
            bulk_uploader.execute_sql_pg(drop_indexes,config)
            logger.info("Dropped indexes from observation_fact")

        for f in glob.glob(bcp_file_path+'/observation_*.bcp'):
            bcp_upload(f, config)
            os.remove(f)
        
        if(str(config.crc_db_type)=='pg'):
            create_indexes = Path('i2b2_cdi/resources/sql') / \
            'create_indexes_observation_fact_pg.sql'        
            if not os.path.exists(create_indexes):
                create_indexes = files('i2b2_cdi.resources.sql').joinpath('create_indexes_observation_fact_pg.sql')          
            bulk_uploader.execute_sql_pg(create_indexes, config)
        logger.success(SUCCESS)
        return factsErrorsList
    except Exception as e:
        logger.error("Failed to load facts : {}", e)
        raise

def bcp_upload(bcp_file_path,config): 
    """Upload the fact data from bcp file to the i2b2 instance

    Args:
        bcp_file_path (:obj:`str`, mandatory): Path to the bcp file having fact data

    """
    logger.debug('entering bcp_upload')
    logger.debug("Uploading facts using BCP")
    base_dir = str(Path(bcp_file_path).parents[2])
    try:
        if(str(config.crc_db_type)=='pg'):
            fact_table_name='observation_fact'
        
        bulkUploader = BulkUploader(
            table_name=fact_table_name,
            import_file=bcp_file_path,
            delimiter=str(config.bcp_delimiter),
            batch_size=10000,
            error_file=base_dir + "/logs/error_bcp_facts.log")

        if(str(config.crc_db_type)=='pg'):
            drop_indexes = Path('i2b2_cdi/resources/sql') / \
            'drop_indexes_observation_fact_pg.sql'
            if os.path.exists(drop_indexes):
                create_indexes = Path('i2b2_cdi/resources/sql') / \
                'create_indexes_observation_fact_pg.sql'
            else:
                drop_indexes = files('i2b2_cdi.resources.sql').joinpath('drop_indexes_observation_fact_pg.sql')
                create_indexes = files('i2b2_cdi.resources.sql').joinpath('create_indexes_observation_fact_pg.sql')          
            
            bulkUploader.execute_sql_pg(drop_indexes,config)
            logger.info("Dropped indexes from observation_fact")
            bulkUploader.upload_facts_pg(config)        
            
        logger.debug('exiting bcp_upload')
        logger.debug('Completed bcp_upload for file:-')
        logger.debug(bcp_file_path)
            
    except Exception as e:
        logger.error("Failed to uplaod facts using BCP : {}", e)
        raise