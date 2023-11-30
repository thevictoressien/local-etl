import os
import csv
import sys
import json
import time
import shutil
from datetime import datetime
from jsonschema import validate
from jsonschema.exceptions import ValidationError



CWD = os.getcwd()  # current working directory
replace_missing_data = True  # Whether to replace missing fields with blanks/NULLs or to discard the data

data = {
    "users": {
        "schema_file": "user-events-schema.json",
        "data_dir": "users",
        "payload_file": "users.csv",
        "metadata_file": "metadata.csv",
        "schema_mismatch_dir": "users_schema_mismatches",
    },

    "cards": {
        "schema_file": "card-events-schema.json",
        "data_dir": "cards",
        "payload_file": "cards.csv",
        "metadata_file": "metadata.csv",
        "schema_mismatch_dir": "cards_schema_mismatches",
    }
}


def create_dir(dir_name: str) -> None:
    """
    Create a directory with the specified name.

    Parameters:
    - dir_name (str): The name of the directory to be created.

    Returns:
    None

    If the directory already exists, no action is taken.
    """
    try:
        os.mkdir(dir_name)
    except IOError:
        pass

def copy_file(fn: str, src: str, dst_dir: str) -> None:
    """
    Copies a file from a source location to a destination directory.

    Parameters:
    - fn (str): The file name (including extension) to be copied.
    - src (str): The source file path.
    - dst_dir (str): The destination directory path.

    Returns:
    None

    If the destination directory does not exist, it will be created before
    copying the file. The function uses the current working directory (CWD)
    as the base path for constructing the full destination path.
    """
    if not os.path.isdir(dst_dir):
        create_dir(dst_dir)
    dst = os.path.join(CWD, dst_dir, fn)
    shutil.copy(src, dst)


def save_log(file_name: str, content: str) -> None:
    """
    Save log content to a file.

    Parameters:
    - file_name (str): The name of the file to save the log to.
    - content (str): The log content to be saved.

    Returns:
    None

    This function attempts to open the specified file in 'append' mode
    and writes the log content to it. If an IOError or PermissionError
    occurs, the function continues trying until it is successful.
    """
    while True:
        try:
            with open(file_name, "a", encoding="utf-8") as output_file:
                output_file.write(content + "\n")
                break
        except (IOError, PermissionError) as error:
            continue


def validate_json(json_data: dict, schema: dict) -> tuple[bool, str]:
    """
    Validate JSON data against a JSON schema.

    Parameters:
    - json_data (dict): The JSON data to be validated.
    - schema (dict): The JSON schema used for validation.

    Returns:
    tuple[bool, str]: A tuple containing a boolean indicating
    whether the validation succeeded and a string message.
    If successful, the boolean is True, and the message is 'success'.
    If validation fails, the boolean is False, and the message
    contains details about the validation error.
    """
    try:  
        validate(instance=json_data, schema=schema)
        return True, 'success'
    except ValidationError as e:
        return False, e.message


def format_time(seconds: float) -> str:
    """
    Format time in seconds into a human-readable string.

    Parameters:
    - seconds (float): The time duration in seconds.

    Returns:
    str: A formatted string representing the time in days, hours, minutes, and seconds.
    If the input is None, returns '-'.
    """
    if seconds is not None:
        seconds = int(seconds)
        d = seconds // (3600 * 24)
        h = seconds // 3600 % 24
        m = seconds % 3600 // 60
        s = seconds % 3600 % 60
        if d > 0:
            return f'{d:02d} day(s), {h:02d} hour(s), {m:02d} minute(s), {s:02d} second(s)'
        elif h > 0:
            return f'{h:02d} hour(s), {m:02d} minute(s) {s:02d} second(s)'
        elif m > 0:
            return f'{m:02d} minute(s), {s:02d} second(s)'
        elif s > 0:
            return f'{s:02d} second(s)'
    return '-'
    

def get_field_names(schema: dict) -> tuple[list, list]:
    """
    Retrieve field names for CSV header from a JSON schema.

    Parameters:
    - schema (dict): The JSON schema.

    Returns:
    tuple[list, list]: A tuple containing two lists:
    - The first list represents payload fields for the CSV header.
    - The second list represents metadata fields for the CSV header.
    """
    # retrieve fields for CSV header from JSON schema
    schema_copy = schema.copy()  # copy dict to prevent modification of referenced dict argument
    payload_fields = []
    metadata_fields = []

    payload_fields.extend(schema_copy['properties']['payload']['required'])
    metadata_fields.extend(schema_copy['properties']['metadata']['required'])

    payload_fields.append('event_id')  # payload foreign key
    
    return payload_fields, metadata_fields


def fix_job_field(job: str) -> str:
    """
    Modify a job title for consistent formatting.

    Parameters:
    - job (str): The original job title.

    Returns:
    str: The modified job title with consistent formatting.
    """
    general_job_title, specialization = job.split(',')

    specialization = specialization.lstrip()  # strip whitespace from specialization
    new_job_title = f'{specialization} {general_job_title}'.capitalize()  # create new title & capitalize

    return new_job_title


def get_row_data(json_data: dict) -> tuple[dict, dict]:
    """
    Retrieve data for a CSV row from JSON data.

    Parameters:
    - json_data (dict): The JSON data.

    Returns:
    tuple[dict, dict]: A tuple containing two dictionaries:
    - The first dictionary represents payload data for the CSV row.
    - The second dictionary represents metadata for the CSV row.
    """
    
    json_data_copy = json_data.copy()
    payload_dict = dict()
    metadata_dict = dict()

    payload_dict.update(json_data_copy['payload'])
    metadata_dict.update(json_data_copy['metadata'])

    # add 'event_id' field to payload_dict
    payload_dict['event_id'] = metadata_dict.get('event_id', '')  # return event_id's value or empty string

    # adhoc data fix for "users"
    if 'address' in payload_dict.keys():  # check if is "users" data to prevent unnecessary overhead for "cards" data
        payload_dict['address'] = payload_dict['address'].replace('\n', ' ')  # strip '\n' (newline character) from address field
        if ',' in payload_dict['job']:  # check if is affected data
            payload_dict['job'] = fix_job_field(payload_dict['job'])


    return payload_dict, metadata_dict


def get_error_log(file_name: str, err_message: str) -> str:
    """
    Create an error log entry.

    Parameters:
    - file_name (str): The name of the file associated with the error.
    - err_message (str): The error message.

    Returns:
    str: A formatted error log entry string.
    """
    time_ = datetime.now().strftime("%d/%m/%Y %I:%M:%S %p")
    error = f'{time_}, ERROR, SCHEMA ERR, {file_name}, {err_message}'
    return error


def main() -> None:
    print('Running...')
    start_time = time.perf_counter()

    for k, v in data.items():
        print(f'\nProcessing {k} data...')
        payload_file = v['payload_file']
        metadata_file = v['metadata_file']
        schema_file = v['schema_file']
        data_dir = v['data_dir']
        bad_data_dir = os.path.join(CWD, v['schema_mismatch_dir'])


        file_count = 0  # total JSON data files
        valid_count = 0  # files that match the schema spec
        invalid_count = 0  # files that don't match the schema spec

        data_path = os.path.join(CWD, data_dir)
        _, _, file_names = next(os.walk(data_path))

        with open(schema_file, 'r') as schema_s:
            schema = json.load(schema_s)

        with open(payload_file, 'a', newline='') as p_file:
            pf, mf = get_field_names(schema)

            # create payload CSV writer object
            p_writer = csv.DictWriter(p_file, fieldnames=pf, restval='', extrasaction='ignore')

            # determine whether to write metadata header
            if p_file.tell() == 0:  # if file object's position is at the beginning of the file, in append mode
                p_writer.writeheader()  # write header row

            # create metadata CSV writer object
            with open(metadata_file, 'a', newline='') as m_file:  # use context manager, to keep file stream open for the duration of data writing
                m_writer = csv.DictWriter(m_file, fieldnames=mf, restval='', extrasaction='ignore')

                if m_file.tell() == 0:
                    m_writer.writeheader()

                for file_name in file_names:
                    if file_name.endswith('.json'):  # parse JSON files only
                        file_count += 1  # increment JSON file count
                    else:
                        continue

                    json_file_path = os.path.join(data_path, file_name)

                    with open(json_file_path, 'r') as json_file:
                        json_data = json.load(json_file)

                    is_valid, message = validate_json(json_data=json_data, schema=schema)

                    if not is_valid:

                        invalid_count += 1
                        error = get_error_log(json_file_path, message)
                        save_log('errors.log', error)  # log error to file for later inspection
                        copy_file(file_name, json_file_path, bad_data_dir)  # copy the file to another directory for later inspection

                        if 'is a required property' not in message:  # if error is anything other than missing field/property
                            continue  # continue to next data

                        if not replace_missing_data:  # discard invalid data
                            continue  # continue to next data
                    else:
                        valid_count += 1

                    p_row_data, m_row_data = get_row_data(json_data)
                    # print(row_dict)
                    p_writer.writerow(p_row_data)
                    m_writer.writerow(m_row_data)

        print(f'Total JSON data files for "{k}": {file_count}')
        print(f'Number of files that match schema: {valid_count}')
        print(f'Number of files with schema errors: {invalid_count}\n')

    time_taken = format_time(time.perf_counter() - start_time)
    print(f'Elapsed Time: {time_taken}')


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("Execution Interrupted!")
        sys.exit()