
#
# Copyright (c) 2020-2021 Massachusetts General Hospital. All rights reserved. 
# This program and the accompanying materials  are made available under the terms 
# of the Mozilla Public License v. 2.0 ( http://mozilla.org/MPL/2.0/) and under 
# the terms of the Healthcare Disclaimer.
#

import os
from pathlib import Path
import csv
from datetime import datetime as DateTime
from i2b2_cdi.database.cdi_database_connections import I2b2crcDataSource
from loguru import logger
from i2b2_cdi.patient.patient_mapping import *
from i2b2_cdi.common import delete_file_if_exists, mkParentDir, file_len
import pandas as pd


class MozillaPatientMapping:
    """The class provides the interface for creating patient mapping i.e. (mapping src encounter id to i2b2 generated encounter num)"""

    def __init__(self,config): 
        self.write_batch_size = 1000
        now = DateTime.now()
        self.patient_num = None
        self.db_patient_map = get_patient_mapping(config)
        self.new_patient_map = {}
        self.rows_skipped = []
        self.import_time = now.strftime('%Y-%m-%d %H:%M:%S')
        self.bcp_header = ['PATIENT_IDE', 'PATIENT_IDE_SRC', 'PATIENT_NUM', 'PATIENT_IDE_STATUS', 'PROJECT_ID',
                           'UPLOAD_DATE', 'UPDATE_DATE', 'DOWNLOAD_DATE', 'IMPORT_DATE', 'SOURCESYSTEM_CD', 'UPLOAD_ID']

    def create_patient_mapping(self, mrn_map_path,  patient_mapping_file_path,fact_mrns_sets,config,rows_skipped=None): 
        """This method creates patient mapping, it checks if mapping already exists
            Accepts mrn_map.csv file with two columns of Mrn : one is the input MRN from the fact file
            and a second optional column of patient_num that should be assigned to the MRN 

        Args:
            mrn_map_path (:obj:`str`, mandatory): Path to the mrn_map file.
            patient_mapping_file_path (:obj:`str`, mandatory): Path to the output csv file.
        """
        try:
            mrn_file_delimiter = str(config.csv_delimiter)
        
            # max lines
            max_line = file_len(mrn_map_path)

            # Get max of patient_num
            self.patient_num = get_max_patient_num(config)

            # Get existing patient mapping
            patient_map = self.db_patient_map
            
            logger.debug("Preparing patient map \n")
            from i2b2_cdi.patient.patient_mapping import get_mrn_list_from_mrn_file, get_patient_mapping_obj
            # Read input csv file
            mrnDf,factFileHeader=get_mrn_list_from_mrn_file(mrn_map_path,mrn_file_delimiter) 

            child_obj =get_patient_mapping_obj(config)

            #codecheck can create new file and import it
            pt_num_list=None
            for mrn_src in factFileHeader:
                if mrn_src=='patient_num':
                    try:
                        pt_num_list=mrnDf['patient_num'].astype(int,errors='raise')
                    except Exception as e:
                        logger.critical("patient_num in mrn_map.csv is not an integer",e)
                else:
                    mrn_list=mrnDf
                    _srcNumLk=get_patient_mapping(config,mrn_src)
                    src = mrn_src

            with open(mrn_map_path, mode='r', encoding='utf-8-sig') as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=mrn_file_delimiter)
                row_number = 0
                header = next(csv_reader)
                # with alive_bar(max_line, bar='smooth') as bar:
                for row in csv_reader:
                    _validation_error = []
                    row_number += 1
                    # Get patient_num if patient already exists
                    patient_num = self.check_if_patient_exists(
                        row, patient_map)

                    # Get next patient_num if it does not exists
                    if patient_num is None:
                        # patient_num = self.get_next_patient_num()
                        self.patient_num = child_obj.prepare_patient_mapping(
                            patient_num, row, _srcNumLk, header,  patient_map,factFileHeader,mrnDf,fact_mrns_sets,self.patient_num)

                        # Print progress
                        # bar()
            print('\n')
            child_obj.write_patient_mapping(
                patient_mapping_file_path, config.source_system_cd, config.upload_id, config.bcp_delimiter)
            rows_skipped=self.rows_skipped
            return rows_skipped
        except Exception as e:
            raise e





    def check_if_patient_exists(self, pt_ids, patient_map):
        """This method checks if patient mapping already exists in a patient_map

        Args:
            pt_ids (:obj:`str`, mandatory): List of src patient ids from different sources.
            patient_map (:obj:`str`, mandatory): Patient map that contains existing mapping.
        """
        patient_num = None
        try:
            for pt_id in pt_ids:
                if pt_id in patient_map:
                    patient_num = patient_map.get(pt_id)
            return patient_num
        except Exception as e:
            raise e


def get_max_patient_num(config):
    """This method runs the query on patient mapping to get max patient_num.
    """
    patient_num = None
    try:
        with I2b2crcDataSource(config) as cursor:
            query = 'select COALESCE(max(patient_num), 0) as patient_num from PATIENT_MAPPING'
            cursor.execute(query)
            row = cursor.fetchone()
            patient_num = row[0]
        return patient_num
    except Exception as e:
        raise e        
        
def get_patient_mapping(config,ide_src=None):
    """Get patient mapping data from i2b2 instance"""
    patient_map = {}
    try:
        logger.debug('Getting existing patient mappings from database')
        query = 'SELECT patient_ide, patient_num FROM patient_mapping'
        if ide_src:
            query+=" where PATIENT_IDE_SOURCE ='"+ide_src+"'"
        with I2b2crcDataSource(config) as (cursor):
            cursor.execute(query)
            result = cursor.fetchall()
            if result:
                for row in result:
                    patient_map.update({row[0]: row[1]})

        return patient_map
    except Exception as e:
        raise Exception("Couldn't get data: {}".format(str(e)))        

def create_patient_mapping(mrn_file_path,config,fact_file=None): 
    
    """This methods contains housekeeping needs to be done before de-identifing patient mrn file.

    Args:
        mrn_file_path (:obj:`str`, mandatory): Path to the input mrn csv file.
    Returns:
        str: Path to converted bcp file
    """
    logger.debug('entering create_patient_mapping')
    logger.debug('Creating patient mapping from mrn file : {}', mrn_file_path)
    fact_mrns_sets = None
    if fact_file is not None:
        for file in fact_file:
            df_fact = pd.read_csv(file)
            fact_mrns_sets = set(df_fact.get('mrn'))
    
    rows_skipped =[]
    if os.path.exists(mrn_file_path):
        D = MozillaPatientMapping(config)
        patient_mapping_file_path = os.path.join(
            Path(mrn_file_path).parent, "deid", "bcp", 'patient_mapping.bcp')

        # Delete bcp and error file if already exists
        delete_file_if_exists(patient_mapping_file_path)
        mkParentDir(patient_mapping_file_path)

        rows_skipped = D.create_patient_mapping(
            mrn_file_path, patient_mapping_file_path,fact_mrns_sets,config)
    
        logger.debug('exiting create_patient_mapping')
        return (patient_mapping_file_path,rows_skipped)
    else:
        logger.error('File does not exist : ', mrn_file_path)
