import os
import csv
import sys
import json
import time
# import toml
import shutil
from datetime import datetime
from jsonschema import validate
from jsonschema.exceptions import ValidationError

# 2 Tables implementation: cards & users

CWD = os.getcwd()  # current working directory
replace_missing_data = True  # Whether to replace missing fields with blanks/NULLs or to discard the data

data = {
    "users": {
        "schema_file": "user-events-schema.json",
        "data_dir": "users",
        "output_file": "users.csv",
        "schema_mismatch_dir": "users_schema_mismatches",
    },

    "cards": {
        "schema_file": "card-events-schema.json",
        "output_file": "cards.csv",
        "data_dir": "cards",
        "schema_mismatch_dir": "cards_schema_mismatches",
    }
}


def copy_file(fn: str, src: str, dst_dir: str) -> None:
    if not os.path.isdir(dst_dir):
        create_dir(dst_dir)
    dst = os.path.join(CWD, dst_dir, fn)
    shutil.copy(src, dst)


def create_dir(dir_name: str) -> None:
    try:
        os.mkdir(dir_name)
    except IOError:
        pass


def save_log(file_name: str, content: str) -> None:

    while True:
        try:
            with open(file_name, "a", encoding="utf-8") as output_file:
                output_file.write(content + "\n")
                break
        except (IOError, PermissionError) as error:
            continue


def validate_json(json_data: dict, schema: dict) -> tuple[bool, str]:

    try:  # try to validate the JSON against schema
        validate(instance=json_data, schema=schema)
        return True, 'success'
    except ValidationError as e:
        return False, e.message


def format_time(seconds: float) -> str:
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
    # retrieve fields for CSV header from JSON schema
    payload_keys = schema['properties']['payload']['required']  # Note: passed by reference
    metadata_keys = schema['properties']['metadata']['required'] # Note: passed by reference

    # define header fields
    fieldnames = list()
    fieldnames.extend(payload_keys)
    fieldnames.extend(metadata_keys)

    return fieldnames


def get_row_data(json_data: dict) -> dict:
    # retrieve JSON fields for CSV row
    payload = json_data['payload'].items()  # Note: passed by reference
    metadata = json_data['metadata'].items() # Note: passed by reference

    row_dict = dict()
    row_dict.update(payload)
    row_dict.update(metadata)

    # adhoc data fix for "users"
    if 'address' in row_dict.keys(): # check if is "users" data to prevent unnecessary overhead for "cards" data
        row_dict['address'] = row_dict['address'].replace('\n', ' ')  # strip '\n' (newline character) from address feild
        if ',' in row_dict['job']:  # check if is affected data
            row_dict['job'] = fix_job_field(row_dict['job'])

    return row_dict


def fix_job_field(job: str) -> str:
    general_job_title, specialization = job.split(',')

    specialization = specialization.lstrip()  # strip whitespace from specialization
    new_job_title = f'{specialization} {general_job_title}'.capitalize()  # create new title & capitalize

    return new_job_title


def get_error_log(file_name: str, err_message: str) -> str:
    time_ = datetime.now().strftime("%d/%m/%Y %I:%M:%S %p")
    error = f'{time_}, ERROR, SCHEMA ERR, {file_name}, {err_message}'
    return error


def main() -> None:
    print('Running...')
    start_time = time.perf_counter()

    for k, v in data.items():
        print(f'\nProcessing {k}\'s data...')
        output_file = v['output_file']
        schema_file = v['schema_file']
        data_dir = v['data_dir']
        bad_data_dir = os.path.join(CWD, v['schema_mismatch_dir'])

        # print(schema_file)

        file_count = 0  # total JSON data files
        valid_count = 0  # files that match the schema spec
        invalid_count = 0  # files that don't match the schema spec

        data_path = os.path.join(CWD, data_dir)
        path, dir_names, file_names = next(os.walk(data_path))

        with open(schema_file, 'r') as schema_s:
            schema = json.load(schema_s)

        with open(output_file, 'a', newline='') as csv_file:
            fieldnames = get_field_names(schema)
            # create CSV writer object
            csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames, restval='', extrasaction='ignore')
            # write header row
            csv_writer.writeheader()

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

                row_dict = get_row_data(json_data)
                # print(row_dict)
                csv_writer.writerow(row_dict)

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