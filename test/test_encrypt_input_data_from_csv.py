import csv
import fnmatch
import hashlib
import importlib
import os
import shutil
import sys

import pytest

CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT_DIR = CURRENT_DIR.rsplit(os.sep, 1)[0]
sys.path.insert(0, PROJECT_ROOT_DIR)

OUTPUT_CSV_COLUMNS = 4
OUTPUT_CSV_ROWS = 2

TESTING_PATH = '/home/hadoop/unit-test'
INPUT_PATH = '/home/hadoop/unit-test/'
OUTPUT_PATH = '/home/hadoop/unit-test/output'
OUTPUT_SUFFIX = '*.csv'

encrypt_input_data_lib = importlib.import_module("encrypt_input_data_from_csv")


def clean_up():
    if os.path.exists(TESTING_PATH):
        shutil.rmtree(TESTING_PATH)


def setup_test_dir():
    os.makedirs(INPUT_PATH)
    os.makedirs(OUTPUT_PATH)

    # remove production settings, will be replaced with test settings in tests
    if os.path.exists(f'{PROJECT_ROOT_DIR}/settings.json'):
        os.remove(f'{PROJECT_ROOT_DIR}/settings.json')


def test_generated_output_files():
    clean_up()
    setup_test_dir()

    # check if testing folder is empty
    assert len(os.listdir(OUTPUT_PATH)) == 0
    shutil.copyfile(f'{CURRENT_DIR}/sample_data/settings.json', f'{PROJECT_ROOT_DIR}/settings.json')
    shutil.copyfile(f'{CURRENT_DIR}/sample_data/input.csv', f'{INPUT_PATH}/input.csv')

    encrypt_input_data_lib.encrypt_input_data()

    # find *.csv files in output folder, should have least 1
    output_files = [f for f in os.listdir(OUTPUT_PATH) if fnmatch.fnmatch(f, OUTPUT_SUFFIX)]
    assert len(output_files) == 1

    clean_up()


def test_output_data_encrypted():
    clean_up()
    setup_test_dir()

    # check if testing folder is empty
    assert len(os.listdir(OUTPUT_PATH)) == 0
    shutil.copyfile(f'{CURRENT_DIR}/sample_data/settings.json', f'{PROJECT_ROOT_DIR}/settings.json')
    shutil.copyfile(f'{CURRENT_DIR}/sample_data/input.csv', f'{INPUT_PATH}/input.csv')

    encrypt_input_data_lib.encrypt_input_data()

    # find *.csv files in output folder, should have least 1
    output_files = [f for f in os.listdir(OUTPUT_PATH) if fnmatch.fnmatch(f, OUTPUT_SUFFIX)]
    assert len(output_files) == 1

    # check if content has been encrypted
    encrypted_first_name = hashlib.sha256('Adaline'.encode()).hexdigest()
    encrypted_last_name = hashlib.sha256('Reichel'.encode()).hexdigest()
    encrypted_address = hashlib.sha256('426 Jordy Lodge'.encode()).hexdigest()
    with open(OUTPUT_PATH + '/' + output_files[0]) as f:
        content = list(csv.reader(f))
        assert content[0][0] == '2000-01-01'
        assert content[0][1] == encrypted_first_name
        assert content[0][2] == encrypted_last_name
        assert content[0][3] == encrypted_address

    clean_up()


def test_output_data_schema():
    clean_up()
    setup_test_dir()

    # check if testing folder is empty
    assert len(os.listdir(OUTPUT_PATH)) == 0
    shutil.copyfile(f'{CURRENT_DIR}/sample_data/settings.json', f'{PROJECT_ROOT_DIR}/settings.json')
    shutil.copyfile(f'{CURRENT_DIR}/sample_data/input.csv', f'{INPUT_PATH}/input.csv')

    encrypt_input_data_lib.encrypt_input_data()

    # find *.csv files in output folder, should have least 1
    output_files = [f for f in os.listdir(OUTPUT_PATH) if fnmatch.fnmatch(f, OUTPUT_SUFFIX)]
    assert len(output_files) == 1

    # check the output only has 4 columns
    with open(OUTPUT_PATH + '/' + output_files[0]) as f:
        content = list(csv.reader(f))
        assert len(content[0]) == OUTPUT_CSV_COLUMNS

    clean_up()


def test_output_data_rows():
    clean_up()
    setup_test_dir()

    # check if testing folder is empty
    assert len(os.listdir(OUTPUT_PATH)) == 0

    shutil.copyfile(f'{CURRENT_DIR}/sample_data/settings.json', f'{PROJECT_ROOT_DIR}/settings.json')
    shutil.copyfile(f'{CURRENT_DIR}/sample_data/input.csv', f'{INPUT_PATH}/input.csv')

    encrypt_input_data_lib.encrypt_input_data()

    # find *.csv files in output folder, should have least 1
    output_files = [f for f in os.listdir(OUTPUT_PATH) if fnmatch.fnmatch(f, OUTPUT_SUFFIX)]
    assert len(output_files) == 1

    # check the output has 2 rows
    with open(OUTPUT_PATH + '/' + output_files[0]) as f:
        content = list(csv.reader(f))
        assert len(content) == OUTPUT_CSV_ROWS

    clean_up()


def test_missing_input():
    clean_up()
    setup_test_dir()
    # check if testing folder is empty
    assert len(os.listdir(OUTPUT_PATH)) == 0

    shutil.copyfile(f'{CURRENT_DIR}/sample_data/settings.json', f'{PROJECT_ROOT_DIR}/settings.json')

    with pytest.raises(encrypt_input_data_lib.FailedToReadInputFile):
        encrypt_input_data_lib.encrypt_input_data()

    # check if there're no output files
    output_files = [f for f in os.listdir(OUTPUT_PATH) if fnmatch.fnmatch(f, OUTPUT_SUFFIX)]
    assert len(output_files) == 0

    clean_up()


def test_invalid_input():
    clean_up()
    setup_test_dir()
    # check if testing folder is empty
    assert len(os.listdir(OUTPUT_PATH)) == 0

    shutil.copyfile(f'{CURRENT_DIR}/sample_data/settings.json', f'{PROJECT_ROOT_DIR}/settings.json')
    shutil.copyfile(f'{CURRENT_DIR}/sample_data/input_missing_columns.csv', f'{INPUT_PATH}/input.csv')

    with pytest.raises(Exception):
        encrypt_input_data_lib.encrypt_input_data()

    # check if there're no output files
    output_files = [f for f in os.listdir(OUTPUT_PATH) if fnmatch.fnmatch(f, OUTPUT_SUFFIX)]
    assert len(output_files) == 0

    clean_up()


def test_missing_settings():
    clean_up()
    setup_test_dir()
    # check if testing folder is empty
    assert len(os.listdir(OUTPUT_PATH)) == 0

    shutil.copyfile(f'{CURRENT_DIR}/sample_data/input.csv', f'{INPUT_PATH}/input.csv')

    with pytest.raises(Exception):
        encrypt_input_data_lib.encrypt_input_data()

    # check if there're no output files
    output_files = [f for f in os.listdir(OUTPUT_PATH) if fnmatch.fnmatch(f, OUTPUT_SUFFIX)]
    assert len(output_files) == 0

    clean_up()


def test_invalid_settings():
    clean_up()
    setup_test_dir()
    # check if testing folder is empty
    assert len(os.listdir(OUTPUT_PATH)) == 0

    shutil.copyfile(f'{CURRENT_DIR}/sample_data/settings_missing_fields.json', f'{PROJECT_ROOT_DIR}/settings.json')
    shutil.copyfile(f'{CURRENT_DIR}/sample_data/input.csv', f'{INPUT_PATH}/input.csv')

    with pytest.raises(Exception):
        encrypt_input_data_lib.encrypt_input_data()

    # check if there're no output files
    output_files = [f for f in os.listdir(OUTPUT_PATH) if fnmatch.fnmatch(f, OUTPUT_SUFFIX)]
    assert len(output_files) == 0

    clean_up()


def test_invalid_output_path():
    clean_up()
    setup_test_dir()
    # check if testing folder is empty
    assert len(os.listdir(OUTPUT_PATH)) == 0

    shutil.copyfile(f'{CURRENT_DIR}/sample_data/settings_invalid_output_path.json', f'{PROJECT_ROOT_DIR}/settings.json')
    shutil.copyfile(f'{CURRENT_DIR}/sample_data/input.csv', f'{INPUT_PATH}/input.csv')

    with pytest.raises(encrypt_input_data_lib.FailedToWriteOutputFiles):
        encrypt_input_data_lib.encrypt_input_data()

    # check if there're no output files
    output_files = [f for f in os.listdir(OUTPUT_PATH) if fnmatch.fnmatch(f, OUTPUT_SUFFIX)]
    assert len(output_files) == 0

    clean_up()


def test_output_files_override():
    clean_up()
    setup_test_dir()

    # check if testing folder is empty
    assert len(os.listdir(OUTPUT_PATH)) == 0
    shutil.copyfile(f'{CURRENT_DIR}/sample_data/settings.json', f'{PROJECT_ROOT_DIR}/settings.json')
    shutil.copyfile(f'{CURRENT_DIR}/sample_data/input.csv', f'{INPUT_PATH}/input.csv')

    encrypt_input_data_lib.encrypt_input_data()

    # find *.csv files in output folder, should have least 1
    output_files = [f for f in os.listdir(OUTPUT_PATH) if fnmatch.fnmatch(f, OUTPUT_SUFFIX)]
    assert len(output_files) == 1

    # check if second time run override the first time run results
    encrypt_input_data_lib.encrypt_input_data()

    # find *.csv files in output folder, should have least 1
    output_files = [f for f in os.listdir(OUTPUT_PATH) if fnmatch.fnmatch(f, OUTPUT_SUFFIX)]
    assert len(output_files) == 1

    clean_up()
