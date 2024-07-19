"""
This is the entrypoint to the program. 'python main.py' will be executed and the 
expected csv file should exist in ../data/destination/ after the execution is complete.
"""

import logging, sys
from pathlib import Path
from typing import Any, List
from src.some_storage_library import SomeStorageLibrary
import uuid

# Change logging level to preferred level.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ROOT = Path(__file__).parent
SOURCE_DATA_DIR = Path(ROOT, 'data', 'source')
DEST_DATA_DIR = Path(ROOT, 'data', 'destination')
SOURCE_COLUMNS = Path(SOURCE_DATA_DIR, 'SOURCECOLUMNS.txt')
SOURCE_DATA = Path(SOURCE_DATA_DIR, 'SOURCEDATA.txt')
DEST_DATA_FILENAME = f'{SOURCE_DATA.stem}_{uuid.uuid1()}.csv'
EXPECTED_ROW_COUNT = 1000
storage_lib = SomeStorageLibrary()

def log_read_file(read_file_func):
    """
    Helper function for read_file function that checks arguments
    and prints relevant log messages to console.
    """
    def wrapper(*args, **kwargs):
        path = kwargs['path']
        strip_whitespace = kwargs['strip_whitespace']

        logging.info(f'Attempting to read file {path}')

        if not path or not isinstance(path, Path):
            logging.error('Invalid path. Provide a valid Path object.')
            return None

        lines = read_file_func(path=path, strip_whitespace=strip_whitespace)
        if not lines:
            logging.warning(f'File, {path} is empty.')
            return None
        
        logging.info(f'Successfully read file {path}')
        
        return lines

    return wrapper

@log_read_file
def read_file(path: Path = None, strip_whitespace: bool = False):
    """
    Reads a file specified by path and returns stripped
    list of lines.

    params:
        path - Path - path to target file

    returns:
        List[str] - list of lines in file
    """

    with open(path, 'r') as f:
        data = f.readlines()

    if strip_whitespace:
        data = [line.strip() for line in data]

    return data

def compose_csv_data(data=None, cols=None) -> List[List[Any]]:
    """
    Composes CSV file using header row and raw data. Assumes headers/data
    have been prepared.

    params:
        data - list of lists containing data rows
        cols - sorted column names

    returns:
        Dictionary containing result data and row count for validations
    """

    if not data or not cols:
        logging.error('Provide valid data and columns.')
        return None

    result = cols + data

    return {'data': result, 'row_count': len(result)}

def sort_by(data, key_index=None):
    """
    Returns sorted list of columns using key_index.

    params:
        data - data to sort
    """

    return sorted(data, key=lambda item: int(item.split('|')[key_index]))

def prep_cols(cols) -> List[str]:
    """
    Prepares the columns by sorting and extracting column names.

    params:
        cols - List[str] - list of column names

    returns: 
        list of sorted colunm names
    """

    cols_sorted = sort_by(cols, 0)
    cols_sorted_names = [col.split('|')[1] for col in cols_sorted]

    return cols_sorted_names

def prep_data(data) -> List[List[Any]]:
    """
    Prepares data by applying certain transformations to each row.

    params:
        data - list of lists of any data type

    returns:
        List of lists of transformed data
    """
    return [','.join(row.split('|')) for row in data]

if __name__ == '__main__':
    """Entrypoint"""
    logging.info('Beginning the ETL process...')

    cols = read_file(path=SOURCE_COLUMNS, strip_whitespace=True)
    if not cols:
        sys.exit()

    prepped_cols_names = [','.join(prep_cols(cols))]
        
    source_data = read_file(path=SOURCE_DATA, strip_whitespace=True)
    if not source_data:
        sys.exit()

    logging.info('Prepping source data...')
    prepped_source_data = prep_data(source_data)

    logging.info('Composing CSV file...')
    source_data_w_headers, row_count = compose_csv_data(data=prepped_source_data, cols=prepped_cols_names).values()

    logging.info('Writing CSV data to file...')
    with open(DEST_DATA_FILENAME, 'w') as f:
        to_write = '\n'.join(source_data_w_headers)
        f.write(to_write)

    logging.info('Running row count verification...')
    if (row_count - 1) != EXPECTED_ROW_COUNT:
        logging.critical('Row counts between source data and CSV file not matching!')
        sys.exit()

    logging.info('Row count verification succeeeded...')

    logging.info('Attempting to move CSV file to destination...')
    storage_lib.load_csv(DEST_DATA_FILENAME)
    logging.info(f'Successfully moved file {DEST_DATA_FILENAME}...')

    logging.info('ETL process complete...')
