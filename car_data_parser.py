import json
import re
import os
from gcp_utils import upload_to_gcs, load_json_files_to_bigquery
from google.cloud import storage
from google.cloud import bigquery


# clean data - while moving lead number into json object
def clean_data(input_file_path, output_directory):
    modified_jsons = []

    output_filename = "jgr_clean_data.json"

    output_file_path = os.path.join(output_directory, output_filename)

    with open(input_file_path, 'r') as file:
        for line in file:
            match = re.match(r'(\d+)(\{.*\})', line.strip())
            if match:
                number, json_str = match.groups()

                try:
                    data = json.loads(json_str)

                    data['timestamp'] = int(number)

                    modified_jsons.append(data)

                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")

    with open(output_file_path, 'w') as output_file:
        json.dump(modified_jsons, output_file, indent=4)

    return output_file_path


# record filter
def filter_create_json(input_file_path, filter_type, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    output_file_path = os.path.join(output_dir, f"{filter_type}.ndjson")

    if os.path.exists(output_file_path):
        print(f"Output file {output_file_path} already exists. It will be overwritten.")

    with open(input_file_path, 'r') as infile, open(output_file_path, 'w') as outfile:
        data = json.load(infile)

        for record in data:
            if record.get('type') == filter_type:
                json.dump(record, outfile)
                outfile.write('\n')

# get distinct types
def get_distinct_types(file_path):
    distinct_types = set()
    type_list = []

    with open(file_path, 'r') as file:
        data = json.load(file)

        for obj in data:
            if 'type' in obj:
                distinct_types.add(obj['type'])

    for type_value in distinct_types:
        type_list.append(type_value)
    return type_list

# variables
input_file_path = '/Users/brenden.visoury/Desktop/Python Projects/jgibbs/SMTSample.json'
output_dir = '/Users/brenden.visoury/Desktop/Python Projects/jgibbs/jgr-folder/jgr-de/data_files/'
bucket_name = 'ds-edw-raw-58cf00aa' # normally this isn't hard coded in
os.environ["GOOGLE_CLOUD_PROJECT"] = "celtic-descent-414215" #neither is this
dataset_id = 'jgr_dev'

clean_data_path = clean_data(input_file_path, output_dir)
filter_types = get_distinct_types(clean_data_path)

for filter_type in filter_types:
    source_file_name = os.path.join(output_dir, f"{filter_type}.ndjson")
    destination_blob_name = f"{filter_type}.ndjson"
    upload_to_gcs(bucket_name, source_file_name, destination_blob_name)

load_json_files_to_bigquery(bucket_name, dataset_id, filter_types)


