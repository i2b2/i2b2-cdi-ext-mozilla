#
# Copyright (c) 2020-2021 Massachusetts General Hospital. All rights reserved. 
# This program and the accompanying materials  are made available under the terms 
# of the Mozilla Public License v. 2.0 ( http://mozilla.org/MPL/2.0/) and under 
# the terms of the Healthcare Disclaimer.
#

import subprocess
from time import time
import subprocess
import os
from datetime import datetime as DateTime
import csv
from time import time


def file_len(fname):
    """Provide the total number of word counts for the specified file

    Args:
       fname (str): name or path of the file for which, the word count to be calculated

    Returns:
        int: count of total number of words from the provided file

    """
    p = subprocess.Popen(['wc', '-l', fname], stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    result, err = p.communicate()
    if p.returncode != 0:
        raise IOError(err)
    return int(result.strip().split()[0])    


def delete_file_if_exists(_file):
    if os.path.exists(_file):
        os.remove(_file)    

def write_deid_file_header(deid_header, deid_file_path, output_deid_delimiter):
    """This method writes the header of deid file using csv writer

    Args:
        deid_file_path (:obj:`str`, mandatory): Path to the deid file.

    """
    try:
        with open(deid_file_path, 'a+') as csvfile:
            writer = csv.DictWriter(
                csvfile, fieldnames=deid_header, delimiter=output_deid_delimiter, lineterminator='\n')
            writer.writeheader()
    except Exception as e:
        raise e

def write_to_deid_file(deid_header,_valid_rows_arr, deid_file_path, output_deid_delimiter):
    """This method writes the list of rows to the deid file using csv writer

    Args:
        _valid_rows_arr (:obj:`str`, mandatory): List of valid facts to be written into deid file.
        deid_file_path (:obj:`str`, mandatory): Path to the output deid file.
        output_deid_delimiter (:obj:`str`, mandatory): Delimeter to be used in deid file.

    """
    try:
        with open(deid_file_path, 'a+') as csvfile:
            writer = csv.DictWriter(
                csvfile, fieldnames=deid_header, delimiter=output_deid_delimiter, lineterminator='\n', extrasaction='ignore')
            writer.writerows(_valid_rows_arr)
    except Exception as e:
        raise e

def write_error_file_header(error_file_header,deid_file_path):
    """This method writes the header of error file using csv writer

    Args:
        deid_file_path (:obj:`str`, mandatory): Path to the error file.

    """
    try:
        with open(deid_file_path, 'a+') as csvfile:
            writer = csv.DictWriter(
                csvfile, fieldnames=error_file_header, delimiter=',', quoting=csv.QUOTE_ALL)
            writer.writeheader()
    except Exception as e:
        raise e                        

def write_to_error_file(error_file_header,error_file_path, _error_rows_arr):
    """This method writes the list of rows to the error file using csv writer

    Args:
        error_file_path (:obj:`str`, mandatory): Path to the error file.
        _error_rows_arr (:obj:`str`, mandatory): List of invalid facts to be written into error file.

    """
    try:
        with open(error_file_path, 'a+') as csvfile:
            writer = csv.DictWriter(
                csvfile, fieldnames=error_file_header, delimiter=',', quoting=csv.QUOTE_ALL, extrasaction='ignore')
            writer.writerows(_error_rows_arr)
    except Exception as e:
        raise e        