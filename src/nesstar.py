# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""NESSTAR Datenmigration."""
import csv
import json
import os
import time
from datetime import datetime
import subprocess as sp

from pyDataverse.api import DataAccessApi, NativeApi
from pyDataverse.models import Datafile, Dataset
from pyDataverse.oaistree import (datafile_from_aip_to_dip,
                                  datafile_from_raw_to_sip,
                                  datafile_from_sip_to_aip,
                                  delete_all_folders_inside, read_history,
                                  save_datafile_dataverse_json,
                                  save_dataset_dataverse_json, save_history,
                                  setup_oaistree, update_csv)
from pyDataverse.utils import (read_csv_as_dicts, read_file, read_json,
                               read_pickle, write_pickle, write_json)

# Settings Instance: Docker Localhost
# NUM_DATASETS = -1
# BASE_URL = 'http://localhost:8085'
# API_TOKEN = '7ab830e0-a493-4e76-96c4-901d4b7ef2bf'
# DATA_DIR = 'data/nesstar/20200324_docker'
# DATA_DIR = 'data/nesstar/20200331_docker'

# Settings Instance: Docker
# NUM_DATASETS = -1
# BASE_URL = 'http://dv02.aussda.at'
# API_TOKEN = 'c0acd5c6-84fe-4a7c-b4cb-1ffd0679086b'
# DATA_DIR = 'data/nesstar/20200324_docker'
# DATA_DIR = 'data/nesstar/20200331_docker'

# Settings Instance: Production
NUM_DATASETS = -1
BASE_URL = 'https://data.aussda.at'
API_TOKEN = 'e5630759-ee17-4a13-a4de-0ece4931faaa'
DATA_DIR = 'data/nesstar/20200425_prod'

# Settings Global
DOI_PREFIX_AUSSDA = 'doi:10.11587'
SEPERATOR = '<s>'
RAW_DIR = 'data/nesstar/raw'
SIP_FOLDERNAME = 'SIP'
AIP_FOLDERNAME = 'AIP'
DIP_FOLDERNAME = 'DIP'
INGEST_FOLDERNAME = 'ingest'
INGEST_DIR = os.path.join(DATA_DIR, INGEST_FOLDERNAME)
FILENAME_DATASETS = os.path.join(DATA_DIR, 'datasets.csv')
FILENAME_DATAFILES = os.path.join(DATA_DIR, 'datafiles.csv')
DATASET_JSON_KEYS = [
    'otherId',
    'series',
    'author',
    'dsDescription',
    'subject',
    'keyword',
    'topicClassification',
    'language',
    'grantNumber',
    'dateOfCollection',
    'kindOfData',
    'dataSources',
    'otherReferences',
    'contributor',
    'relatedDatasets',
    'relatedMaterial',
    'datasetContact',
    'distributor',
    'producer',
    'publication',
    'software',
    'timePeriodCovered',
    'geographicUnit',
    'geographicBoundingBox',
    'geographicCoverage',
    'socialScienceNotes',
    'unitOfAnalysis',
    'universe',
    'targetSampleActualSize'
]
DATAFILE_JSON_KEYS = [
    'categories'
]


def clean_string(str):
    clean_str = str.strip()
    clean_str = clean_str.replace('  ', ' ')
    return clean_str


def import_datasets(datasets_csv):
    data = {}
    # license_default_en = read_file(os.path.join(DATA_DIR, LICENSE_EN))

    for dataset in datasets_csv:
        ds_tmp = {}

        for key, val in dataset.items():
            if val:
                val = clean_string(val)
                if val == 'TRUE':
                    val = True
                elif val == 'FALSE':
                    val = False
                key_split = key.split('.')
                if key_split[0] == 'dv':
                    real_key = key_split[1]
                    if real_key == 'otherId':
                        val = val.replace('!', '_')
                    if real_key in DATASET_JSON_KEYS:
                        ds_tmp[real_key] = json.loads(val)
                    else:
                        ds_tmp[real_key] = val
                elif key_split[0] == 'org':
                    if key == 'org.dataset_id':
                        ds_id = val
                    elif key == 'org.dataverse_id':
                        ds_tmp['dataverse_id'] = val
                    else:
                        ds_tmp[key] = val
                else:
                    ds_tmp[key] = val
        if 'dataverse_id' in ds_tmp:
            data[ds_id] = {'metadata': ds_tmp}
    print('- Import Datasets COMPLETED.')
    return data


def import_datafiles(data, datafiles_csv):

    for datafile in datafiles_csv:
        df_tmp = {}
        df_id = None
        if datafile['org.to_upload'] == 'TRUE':
            for key, val in datafile.items():
                if val:
                    # TODO: Update read CSV file settings to auto-import boolean variables.
                    if val == 'TRUE':
                        val = True
                    elif val == 'FALSE':
                        val = False
                    key_split = key.split('.')
                    if key_split[0] == 'dv':
                        real_key = key_split[1]
                        if real_key in DATAFILE_JSON_KEYS:
                            df_tmp[real_key] = json.loads(clean_string(val))
                        else:
                            if real_key == 'title':
                                val = val.replace(';', ' - ')
                                val = val.replace('\'', '\\\'')
                            if isinstance(val, str):
                                df_tmp[real_key] = clean_string(val)
                    elif key_split[0] == 'org':
                        # df_tmp[key] = val
                        if key == 'org.datafile_id':
                            df_id = clean_string(val)
                        elif key == 'org.dataset_id':
                            ds_id = clean_string(val)
                        elif key == 'org.filename':
                            df_tmp['filename'] = clean_string(val)
            if ds_id in data:
                if 'datafiles' not in data[ds_id]:
                    data[ds_id]['datafiles'] = {}
                if df_id not in data[ds_id]['datafiles']:
                    data[ds_id]['datafiles'][df_id] = {}
                if 'metadata' not in data[ds_id]['datafiles'][df_id]:
                    data[ds_id]['datafiles'][df_id]['metadata'] = {}

                data[ds_id]['datafiles'][df_id]['metadata'] = df_tmp

    print('- Import Datafiles COMPLETED.')
    return data


def setup_dirs(data, delete_all_folders=False, overwrite_all=False):
    counter = 0
    if delete_all_folders:
        delete_all_folders_inside(INGEST_DIR)

    for ds_id, dataset in data.items():
        counter += 1
        ds_dir = os.path.join(INGEST_DIR, ds_id)
        setup_oaistree(ds_dir, ds_id,
                       os.path.join(DATA_DIR, 'terms-of-use_suf_v1.4.html'),
                       os.path.join(DATA_DIR, 'terms-of-access_suf_v1.4.html'),
                       overwrite_all)
        if 'datafiles' in data[ds_id]:
            for df_id, datafile in data[ds_id]['datafiles'].items():
                datafile_from_raw_to_sip(
                    os.path.join(RAW_DIR, datafile['metadata']['filename']),
                    os.path.join(ds_dir, SIP_FOLDERNAME, datafile['metadata']['filename']),
                    overwrite_all=overwrite_all)
                datafile_from_sip_to_aip(
                    os.path.join(ds_dir, SIP_FOLDERNAME, datafile['metadata']['filename']),
                    os.path.join(ds_dir, AIP_FOLDERNAME, datafile['metadata']['filename']),
                    overwrite_all=overwrite_all)
                if 'Documentation' in datafile['metadata']['categories']:
                    category = 'documentation'
                elif 'Data' in datafile['metadata']['categories']:
                    category = 'data'
                else:
                    category = None
                datafile_from_aip_to_dip(
                    ds_dir, datafile['metadata']['filename'],
                    overwrite_all=overwrite_all)
        if counter >= NUM_DATASETS and NUM_DATASETS >= 0:
            break
    print('- Setup OAIS trees COMPLETED.')


def create_datasets_json(data):
    counter = 0
    for ds_id, dataset in data.items():
        counter += 1
        ds_dir = os.path.join(INGEST_DIR, ds_id)
        save_dataset_dataverse_json(ds_dir, dataset['metadata'], ds_id)
        if counter >= NUM_DATASETS and NUM_DATASETS >= 0:
            break
    print('- Create Datasets JSON COMPLETED.')


def upload_datasets(data, filename_datasets):
    counter = 0
    update_dict = {}

    api = NativeApi(BASE_URL, API_TOKEN)
    for ds_id, dataset in data.items():
        pid = None
        counter += 1
        ds_dir = os.path.join(INGEST_DIR, ds_id)
        history = read_history(ds_dir)
        if 'upload_date' in history:
            if history['upload_date']:
                do_upload = False
                print('Dataset {0} already uploaded.'.format(ds_id))
            else:
                do_upload = True
        else:
            do_upload = True
        if do_upload:
            try:
                ds = Dataset()
                ds.set(dataset['metadata'])
                ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                resp = api.create_dataset(dataset['metadata']['dataverse_id'], ds.json())
                if 'status' in resp.json():
                    if resp.json()['status'] == 'OK':
                        history['upload_date'] = ts
                        if 'data' in resp.json():
                            if 'persistentId' in resp.json()['data']:
                                pid = resp.json()['data']['persistentId']
                                history['pid'] = pid
                                save_history(ds_dir, history)
                                update_dict[ds_id] = {'org.doi': pid, 'org.is_uploaded': 'TRUE'}
                            elif 'id' in resp.json()['data']:
                                dataset_id = resp.json()['data']['id']
                                history['dataverse_datasetId'] = str(dataset_id)
                                resp = api.get_dataset(dataset_id, is_pid=False)
                                pid = DOI_PREFIX_AUSSDA + '/' + resp.json()['data']['identifier']
                                history['pid'] = pid
                                save_history(ds_dir, history)
                                update_dict[ds_id] = {'org.doi': pid, 'org.is_uploaded': 'TRUE'}
                            else:
                                print('ERROR: Create Dataset {0} - no \'persistentId\' in API response.'.format(ds_id))
                        else:
                            print('ERROR: Create Dataset {0} - no \'data\' in API response.'.format(ds_id))
                    else:
                        print('ERROR: Create Dataset {0} API Request Status not OK'.format(ds_id))
                else:
                    print('ERROR: Create Dataset {0} API Request not working.'.format(ds_id))
            except:
                print('Dataset {0} could not be created.'.format(ds_id))
            time.sleep(1)
        else:
            pass
        if counter >= NUM_DATASETS and NUM_DATASETS >= 0:
            break
    update_csv(filename_datasets, update_dict)
    print('- Upload Datasets COMPLETED.')
    return data


def create_datafiles_json(data):
    counter = 0
    for ds_id, dataset in data.items():
        counter += 1
        ds_dir = os.path.join(INGEST_DIR, ds_id)
        history = read_history(ds_dir)
        if 'pid' in history:
            if 'datafiles' in dataset:
                pid = history['pid']
                for df_id, datafile in dataset['datafiles'].items():
                    datafile['metadata']['pid'] = pid
                    save_datafile_dataverse_json(ds_dir, datafile['metadata'], ds_id, df_id)
            else:
                print('WARNING: No Datafile entry for Dataset {0} available.'.format(ds_id))
        else:
            print('WARNING: PID for Dataset {0} not available.'.format(ds_id))
        if counter >= NUM_DATASETS and NUM_DATASETS >= 0:
            break
    print('- Create Datafiles COMPLETED.')


def upload_datafiles(data, filename_datafiles):
    counter = 0
    update_dict = {}
    api = NativeApi(BASE_URL, API_TOKEN)

    for ds_id, dataset in data.items():
        pid = None
        counter += 1
        ds_dir = os.path.join(INGEST_DIR, ds_id)
        history = read_history(ds_dir)
        if 'pid' in history:
            if 'datafiles' in dataset:
                pid = history['pid']
                for df_id, datafile in dataset['datafiles'].items():
                    history = read_history(ds_dir)
                    if 'datafiles' in history:
                        if df_id in history['datafiles']:
                            if 'upload_date' in history['datafiles'][df_id]:
                                if history['datafiles'][df_id]['upload_date']:
                                    do_upload = False
                                else:
                                    do_upload = True
                            else:
                                do_upload = True
                        else:
                            do_upload = True
                    else:
                        do_upload = True
                    # api_dataset_is_accessible = api.wait_api_dataset_lock(pid) # not working with Dataverse versions before 4.9.3
                    if do_upload:
                        try:
                            data_tmp = datafile['metadata']
                            df = Datafile()
                            data_tmp['pid'] = pid
                            df.set(data_tmp)
                            filename = os.path.abspath(os.path.join(ds_dir, DIP_FOLDERNAME, datafile['metadata']['filename']))
                            ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            resp = api.upload_datafile(pid, filename, json_str=df.json(), is_pid=True)
                            if 'status' in resp.json():
                                if resp.json()['status'] == 'OK':
                                    print('Datafile {0} uploaded.'.format(df_id))
                                    if 'datafiles' not in history:
                                        history['datafiles'] = {}
                                    if df_id not in history['datafiles']:
                                        history['datafiles'][df_id] = {}
                                    history['datafiles'][df_id]['upload_date'] = ts
                                    history['datafiles'][df_id]['filename'] = datafile['metadata']['filename']
                                    update_dict[ds_id] = {'org.is_uploaded': 'TRUE'}
                                    save_history(ds_dir, history)
                                else:
                                    print('ERROR: Upload Datafile {0} API response status not OK. - MSG: {1}.'.format(df_id, resp.json()))
                            else:
                                print('ERROR: Upload Datafile {0} API response not valid.'.format(df_id))
                        except:
                            print('WARNING: Datafile {0} could not be uploaded.'.format(df_id))
                        if filename[-4:] == '.sav' or filename[-4:] == '.dta':
                            time.sleep(30)
                        else:
                            time.sleep(2)
                    else:
                        print('Datafile {0} already uploaded.'.format(df_id))
            else:
                print('No Datafile for Dataset {0} available.'.format(ds_id))
        else:
            print('ERROR: Upload Datafile - PID {0} not available.'.format(ds_id))
        if counter >= NUM_DATASETS and NUM_DATASETS >= 0:
            break
    update_csv(filename_datafiles, update_dict)
    print('- Upload Datafiles COMPLETED.')
    return data


def destroy_datasets(data):
    counter = 0
    api = NativeApi(BASE_URL, API_TOKEN)

    for ds_id, dataset in data.items():
        counter += 1
        ds_dir = os.path.join(INGEST_DIR, ds_id)
        history = read_history(ds_dir)
        if 'pid' in history:
            pid = history['pid']
            if 'destruction_date' in history:
                if history['destruction_date']:
                    do_destroy = False
                else:
                    do_destroy = True
            else:
                do_destroy = True
            if do_destroy:
                try:
                    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    resp = api.destroy_dataset(pid)
                    if 'status' in resp.json():
                        if resp.json()['status'] == 'OK':
                            if 'data' in resp.json():
                                history['destruction_date'] = ts
                                save_history(ds_dir, history)
                            else:
                                print('ERROR: Destroy Dataset {0} - No data in API response.'.format(pid))
                        else:
                            print('ERROR: Destroy Dataset {0} API request status not OK.'.format(pid))
                except:
                    print('Dataset {0} could not be destroyed.'.format(pid))
                time.sleep(1)
            else:
                print('Dataset {0} can not be destroyed.'.format(pid))
        if counter >= NUM_DATASETS and NUM_DATASETS >= 0:
            break
    print('- Destroy Datasets COMPLETED.')


def publish_datasets(data, filename_datasets):
    counter = 0
    update_dict = {}
    api = NativeApi(BASE_URL, API_TOKEN)

    for ds_id, dataset in data.items():
        pid = None
        counter += 1
        ds_dir = os.path.join(INGEST_DIR, ds_id)
        history = read_history(ds_dir)
        if dataset['metadata']['org.to_publish'] and not dataset['metadata']['org.is_published']:
            do_publish = True
        else:
            do_publish = False
        pid = dataset['metadata']['org.doi']
        if do_publish:
            try:
                ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                resp = api.publish_dataset(pid, 'major')
                if 'status' in resp.json():
                    if resp.json()['status'] == 'OK':
                        if 'data' in resp.json():
                            # history['publishing_date'] = ts
                            # save_history(ds_dir, history)
                            update_dict[ds_id] = {'org.is_published': 'TRUE'}
                        else:
                            print('ERROR: Publish Dataset {0} - no data in API response.'.format(pid))
                    else:
                        print('ERROR: Publish Dataset {0} API request status not OK.'.format(pid))
            except:
                print('Dataset {0} could not be published.'.format(pid))
            time.sleep(1)
        else:
            print('Dataset {0} can not be published.'.format(pid))
        if counter >= NUM_DATASETS and NUM_DATASETS >= 0:
            break
    update_csv(filename_datasets, update_dict)
    print('- Publish Datasets COMPLETED.')


def delete_datasets(data):
    counter = 0
    api = NativeApi(BASE_URL, API_TOKEN)

    for ds_id, dataset in data.items():
        counter += 1
        ds_dir = os.path.join(INGEST_DIR, ds_id)
        history = read_history(ds_dir)
        if 'pid' in history:
            pid = history['pid']
            if 'deletion_date' in history:
                if history['deletion_date']:
                    do_deletion = False
                else:
                    do_deletion = True
            else:
                do_deletion = True
            if do_deletion:
                try:
                    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    resp = api.delete_dataset(pid)
                    if 'status' in resp.json():
                        if resp.json()['status'] == 'OK':
                            if 'data' in resp.json():
                                history['deletion_date'] = ts
                                save_history(ds_dir, history)
                            else:
                                print('ERROR: Delete Dataset {0} - no data in API response.'.format(pid))
                        else:
                            print('ERROR: Delete Dataset {0} - API request status not OK.'.format(pid))
                except:
                    print('Dataset \'{0}\' could not be deleted.'.format(pid))
                time.sleep(2)
            else:
                print('Dataset {0} can not be deleted.'.format(pid))
        if counter >= NUM_DATASETS and NUM_DATASETS >= 0:
            break
    print('- Delete Datasets COMPLETED.')


def update_datasets(data, filename_updated_csv):
    counter = 0
    update_dict = {}
    api = NativeApi(BASE_URL, API_TOKEN)

    for dataset in data:
        counter += 1
        if dataset['org.to_update'] == 'TRUE' and dataset['org.is_updated'] == 'FALSE':
            do_update = True
        else:
            do_update = False

        if do_update:
            ds_id = dataset['org.dataset_id']
            pid = dataset['org.doi']
            geo_csv = json.loads(dataset['dv.geographicCoverage'])
            g_list = []
            for g in geo_csv:
                d_dict = {}
                for key, val in g.items():
                    d_dict[key] = {}
                    d_dict[key]['typeName'] = key
                    d_dict[key]['value'] = val
                g_list.append(d_dict)
            data = {
                "fields": [
                    {
                        "typeName": "geographicCoverage",
                        "value": g_list
                    }
                ]
            }
            ds_dir = os.path.join(INGEST_DIR, ds_id)
            history = read_history(ds_dir)
            if 'pid' in history:
                try:
                    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    resp = api.edit_dataset_metadata(pid, json.dumps(data), replace=True)
                    if 'status' in resp.json():
                        if resp.json()['status'] == 'OK':
                            if 'data' in resp.json():
                                if 'update_date' not in history:
                                    history['update_date'] = []
                                history['update_date'].append(ts)
                                update_dict[ds_id] = {'org.is_updated': 'TRUE', 'org.to_update': 'FALSE'}
                                save_history(ds_dir, history)
                            else:
                                print('ERROR: Update Dataset {0} - no data in API response.'.format(pid))
                        else:
                            print('ERROR: Update Dataset {0} - API request status not OK. MSG: {1}'.format(pid, resp.json()['status']))
                    else:
                        print('ERROR: Update Dataset {0} - API request status not available.'.format(pid))
                except:
                    print('Dataset \'{0}\' could not be updated.'.format(pid))
            else:
                print('Dataset \'{0}\' is not been uploaded.'.format(pid))
            time.sleep(2)
        if counter >= NUM_DATASETS and NUM_DATASETS >= 0:
            break
    update_csv(filename_updated_csv, update_dict)
    print('- Update Datasets COMPLETED.')


def update_datafiles(datafiles_update_csv, datafiles_csv, datasets_csv):
    df_id_lst = []
    df_lst = []
    ds_id_lst = []
    ds_lst = []
    data = []
    df_attr_upload = [
        'description',
        'categories',
        'restrict',
        'directoryLabel',
        'label'
    ]
    api = NativeApi(BASE_URL, API_TOKEN)
    api_da = DataAccessApi(BASE_URL, API_TOKEN)

    # # collect all datafile IDs
    # for df in datafiles_update_csv:
    #     if df['dv.restrict'] == 'TRUE':
    #         df_id_lst.append(df['org.datafile_id'])
    # # collect datafile data
    # for df in datafiles_csv:
    #     if df['org.datafile_id'] in df_id_lst:
    #         df_lst.append({
    #             'org.datafile_id': df['org.datafile_id'],
    #             'filename': df['org.filename'],
    #             'dataset_id': df['org.dataset_id']
    #         })
    #         if df['org.dataset_id'] not in ds_id_lst:
    #             ds_id_lst.append(df['org.dataset_id'])
    #
    # # collect PID's
    # for ds in datasets_csv:
    #     if ds['org.dataset_id'] in ds_id_lst:
    #         ds_lst.append({
    #             'dataset_id': ds['org.dataset_id'],
    #             'pid': ds['org.doi']
    #         })
    #
    # # allow access request
    # for ds in ds_lst:
    #     resp = api_da.allow_access_request(ds['pid'])
    #     print(resp.text)

    # # update datafile metadata
    # for ds in ds_lst:
    #     resp = api.get_datafiles(ds['pid'], version=':latest')
    #     dv_files = resp.json()['data']
    #     for dv_file in dv_files:
    #         for idx, df_file in enumerate(df_lst):
    #             if dv_file['label'] == df_file['filename']:
    #                 df_lst[idx]['dv.datafile_id'] = dv_file['dataFile']['id']
    #             elif df_file['filename'].split('.')[1] == 'dta' and dv_file['label'].split('.')[1] == 'tab' and dv_file['label'].split('.')[0] == df_file['filename'].split('.')[0]:
    #                 df_lst[idx]['dv.datafile_id'] = dv_file['dataFile']['id']
    #                 df_lst[idx]['filename'] = dv_file['label']
    # print(json.dumps(df_lst))
    # df_lst = read_json(os.path.join(DATA_DIR, 'df_lst.json'))
    # for df in df_lst:
    #     resp = api.get_datafile_metadata(df['dv.datafile_id'], is_draft=True)
    #     data_resp = resp.json()
    #     dict_tmp = {
    #         'restrict': True
    #     }
    #     for key, val in data_resp.items():
    #         if key in df_attr_upload:
    #             dict_tmp[key] = val
    #     data.append([dict_tmp, df['dv.datafile_id']])
    # print(json.dumps(data))

    # data = read_json(os.path.join(DATA_DIR, 'df_metadata.json'))
    # for df_data, df_id in data[:]:
    #     resp = api.update_datafile_metadata(df_id, json_str=json.dumps(df_data), is_filepid=False)
    #     print(resp)


if __name__ == '__main__':
    print('START --------------------------')

    # Workflow Control
    CREATE_1 = False
    CREATE_2 = False
    CREATE_3 = False
    DELETE = False
    PUBLISH = False
    REDETECT_DATATYPE = True
    UPDATE_DATASETS = False
    UPDATE_DATAFILES = False

    if CREATE_1:
        datasets_csv = read_csv_as_dicts(FILENAME_DATASETS, delimiter=',')
        data = import_datasets(datasets_csv)
        datafiles_csv = read_csv_as_dicts(FILENAME_DATAFILES, delimiter=',')
        data = import_datafiles(data, datafiles_csv)
        write_pickle(os.path.join(DATA_DIR, 'import_datafiles.pickle'), data)
        setup_dirs(data, delete_all_folders=False, overwrite_all=False)
    if CREATE_2:
        data = read_pickle(os.path.join(DATA_DIR, 'import_datafiles.pickle'))
        create_datasets_json(data)
        data = upload_datasets(data, FILENAME_DATASETS)
    if CREATE_3:
        data = read_pickle(os.path.join(DATA_DIR, 'import_datafiles.pickle'))
        create_datafiles_json(data)
        data = upload_datafiles(data, FILENAME_DATAFILES)
    if PUBLISH:
        datasets_csv = read_csv_as_dicts(FILENAME_DATASETS, delimiter=',')
        data = import_datasets(datasets_csv)
        write_pickle(os.path.join(DATA_DIR, 'publish_datasets.pickle'), data)
        data = read_pickle(os.path.join(DATA_DIR, 'publish_datasets.pickle'))
        publish_datasets(data, FILENAME_DATASETS)
    if DELETE:
        data = read_pickle(os.path.join(DATA_DIR, 'import_datafiles.pickle'))
        delete_datasets(data)
    if REDETECT_DATATYPE:
        # df_id_lst = []
        # datasets_csv = read_csv_as_dicts(FILENAME_DATASETS, delimiter=',')
        # api = NativeApi(BASE_URL, API_TOKEN)
        # for ds in datasets_csv:
        #     resp = api.get_datafiles(ds['org.doi'])
        #     for df in resp.json()['data']:
        #         if df['dataFile']['filename'].split('.')[1] == 'pdf' and df['dataFile']['contentType'] != 'application/pdf':
        #             df_id_lst.append(str(df['dataFile']['id']))
        # write_json('df_id.json', df_id_lst)
        df_id_lst = read_json('df_id.json')
        for df in df_id_lst:
            query = '{0}/api/files/{1}/redetect?dryRun=false'.format(BASE_URL, df)
            shell_command = 'curl -H "X-Dataverse-key: {0}"'.format(API_TOKEN)
            shell_command += ' -X POST {0}'.format(query)
            result = sp.run(shell_command, shell=True, stdout=sp.PIPE)
            resp_dict = json.loads(result.stdout)
            if resp_dict['status'] != 'OK':
                print('\n', resp_dict)
    if UPDATE_DATASETS:
        # 1. get all file ids via get_dataset() call
        filename_updated_csv = os.path.join(DATA_DIR, 'datasets_updated.csv')
        data = read_csv_as_dicts(filename_updated_csv, delimiter=',')
        update_datasets(data, filename_updated_csv)
    if UPDATE_DATAFILES:
        datafiles_update_csv = read_csv_as_dicts(os.path.join(DATA_DIR, 'datafiles_updated.csv'), delimiter=',')
        datafiles_csv = read_csv_as_dicts(FILENAME_DATAFILES, delimiter=',')
        datasets_csv = read_csv_as_dicts(FILENAME_DATASETS, delimiter=',')
        update_datafiles(datafiles_update_csv, datafiles_csv, datasets_csv)
    print('END ----------------------------')
