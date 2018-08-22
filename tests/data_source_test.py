# -*- coding: utf-8 -*-
import json
import os

from flask import url_for

from limonero.models import DataSource


def test_data_source_list_fail_not_authorized(client):
    rv = client.get('/datasources')
    assert 401 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)
    resp = json.loads(rv.data)
    assert resp['status'] == 'ERROR', 'Incorrect status'
    assert 'Thorn' in resp['message'], 'Incorrect message'


def test_data_source_list_success(client, auth_token, default_ds):
    rv = client.get('/datasources', headers={'X-Auth-Token': auth_token})
    assert 200 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)
    resp = json.loads(rv.data)
    assert len(resp['data']) == 1, 'Wrong quantity'

    assert resp['data'][0]['id'] == default_ds.id
    assert resp['data'][0]['format'] == default_ds.format
    assert resp['data'][0]['url'] == default_ds.url
    assert resp['data'][0]['name'] == default_ds.name
    assert resp['data'][0]['enabled'] == default_ds.enabled


def test_data_source_list_many_success(client, auth_token, datasources):
    rv = client.get('/datasources', headers={'X-Auth-Token': auth_token})
    assert 200 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)
    resp = json.loads(rv.data)
    assert len(resp['data']) == len(datasources), 'Wrong quantity'


def test_data_source_get_success(client, auth_token, default_ds):
    rv = client.get('/datasources/{}'.format(default_ds.id),
                    headers={'X-Auth-Token': auth_token})

    assert 200 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)
    resp = json.loads(rv.data)
    assert resp['id'] == default_ds.id
    assert resp['format'] == default_ds.format
    assert resp['url'] == default_ds.url
    assert resp['name'] == default_ds.name
    assert resp['enabled'] == default_ds.enabled


def test_data_source_not_found_error(client, auth_token):
    rv = client.get('/datasources/{}'.format(999),
                    headers={'X-Auth-Token': auth_token})
    assert 404 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)


def test_data_source_get_simple_success(client, auth_token, default_ds):
    rv = client.get('/datasources',
                    query_string={'simple': 'true'},
                    headers={'X-Auth-Token': auth_token})

    assert 200 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)
    resp = json.loads(rv.data)['data'][0]
    assert resp.keys() == [u'user_id', u'description', u'created',
                           u'privacy_aware', u'user_name', u'id', u'name']


def test_data_source_get_fields_success(client, auth_token, default_ds):
    rv = client.get('/datasources',
                    query_string={'fields': 'name,id'},
                    headers={'X-Auth-Token': auth_token})

    assert 200 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)
    resp = json.loads(rv.data)['data'][0]
    assert resp.keys() == [u'id', u'name']


def test_data_source_get_fields_invalid_sort_success(client, auth_token,
                                                     default_ds):
    rv = client.get('/datasources',
                    query_string={'fields': 'name,id', 'sort': 'wrong',
                                  'asc': 'false'},
                    headers={'X-Auth-Token': auth_token})

    assert 200 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)
    resp = json.loads(rv.data)['data'][0]
    assert resp.keys() == [u'id', u'name']


def test_data_source_simple_list_success(client, auth_token, default_ds):
    rv = client.get('/datasources',
                    query_string={'list': 'true'},
                    headers={'X-Auth-Token': auth_token})

    assert 200 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)
    resp = json.loads(rv.data)[0]
    assert resp.keys() == [u'id', u'name']


def test_data_source_non_existing_field_fail(client, auth_token, default_ds):
    rv = client.get('/datasources',
                    query_string={'fields': 'non-existing'},
                    headers={'X-Auth-Token': auth_token})

    assert 500 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)
    resp = json.loads(rv.data)
    assert resp['status'] == 'ERROR'


def test_create_data_source_success(client, auth_token, app,
                                    default_storage):
    payload = {
        'name': 'Data source 1',
        'format': 'JSON',
        'is_public': False,
        'storage_id': 1,
        'url': 'hdfs://dev:9000/data/test.json'

    }
    rv = client.post('/datasources',
                     data=json.dumps(payload),
                     content_type='application/json',
                     headers={'X-Auth-Token': auth_token})

    assert 200 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)
    resp = json.loads(rv.data)
    with app.app_context():
        ds = DataSource.query.get(resp['id'])
        assert ds is not None, 'Data source was not created'


def test_delete_data_source_success(client, auth_token, app, default_ds):
    rv = client.delete('/datasources/{}'.format(default_ds.id),
                       headers={'X-Auth-Token': auth_token})

    assert 200 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)
    resp = json.loads(rv.data)
    assert resp['status'] == 'OK'
    assert 'successfuly deleted' in resp['message']


def test_update_data_source_success(client, auth_token, app, default_ds):
    payload = {
        'name': 'New name for data source',
        'format': 'CSV',
        'is_public': True,
        'storage_id': 1,
        'url': 'hdfs://dev:9000/data/test.parquet'

    }
    rv = client.patch('/datasources/{}'.format(default_ds.id),
                      content_type='application/json',
                      data=json.dumps(payload),
                      headers={'X-Auth-Token': auth_token})

    assert 200 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)

    resp = json.loads(rv.data)
    assert resp['status'] == 'OK'
    assert 'successfuly updated' in resp['message']
    ds = DataSource.query.get(default_ds.id)
    for k, v in payload.items():
        assert getattr(ds, k) == v, \
            'Value differ after update: {} != {}'.format(getattr(ds, k), v)


def test_update_data_source_not_found_fail(client, auth_token):
    payload = {
        'name': 'New name for data source',
        'format': 'CSV',
        'is_public': True,
        'storage_id': 1,
        'url': 'hdfs://dev:9000/data/test.parquet'

    }
    rv = client.patch('/datasources/{}'.format(1),
                      content_type='application/json',
                      data=json.dumps(payload),
                      headers={'X-Auth-Token': auth_token})

    assert 400 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)

    resp = json.loads(rv.data)
    assert resp['status'] == 'ERROR'
    assert 'not found' in resp['message']


def test_data_source_infer_simple_schema_success(client, auth_token, jvm, app,
                                                 infer_ds):
    """ Uses JVM !"""

    for ds, meta in infer_ds:
        url = url_for('DataSourceInferSchemaApi', data_source_id=ds.id)
        rv = client.post(url, content_type='application/json',
                         headers={'X-Auth-Token': auth_token})
        assert 200 == rv.status_code, 'Incorrect status code: {}'.format(
            rv.data)
        with app.app_context():
            read_ds = DataSource.query.get(ds.id)
            assert len(meta) == len(read_ds.attributes)
            for i, (name, data_type) in enumerate(meta):
                assert data_type == read_ds.attributes[
                    i].type, '{} x {}'.format(name, read_ds.attributes[i].name)


def test_data_source_check_chunk_dont_exist_success(client, auth_token, jvm,
                                                    file_storage):
    url = url_for('DataSourceUploadApi')
    params = {
        'resumableIdentifier': 'eda-cc0-ffa',
        'resumableFilename': 'new_data_source.csv',
        'resumableChunkNumber': 1,
        'storage_id': file_storage.id
    }
    rv = client.get(url, content_type='application/json',
                    query_string=params,
                    headers={'X-Auth-Token': auth_token})
    assert 404 == rv.status_code, 'Incorrect status code: {}'.format(
        rv.data)


def test_data_source_upload_chunk_success(client, auth_token, jvm, app,
                                          file_storage):
    url = url_for('DataSourceUploadApi')
    params = {
        'resumableIdentifier': 'eda-cc0-ffa',
        'resumableFilename': 'new_data_source.csv',
        'resumableChunkNumber': 1,
        'resumableTotalChunks': 1,
        'resumableTotalSize': 20,
        'storage_id': file_storage.id
    }
    data = "id,name\n1,bob\n2,Alice"
    rv = client.post(url, content_type='application/json',
                     data=data,
                     query_string=params,
                     headers={'X-Auth-Token': auth_token})
    assert 200 == rv.status_code, 'Incorrect status code: {}'.format(
        rv.data)
    with app.app_context():
        ds = DataSource.query.get(1)
        assert os.path.exists(ds.url[5:])
        with open(ds.url[5:]) as f:
            assert f.read() == data
        os.unlink(ds.url[5:])


def test_data_source_upload_chunk_hdfs_success(client, auth_token, jvm, app,
                                               default_storage, hdfs, db):
    cluster, uri, fs = hdfs
    with app.app_context():
        default_storage.url = uri
        db.session.add(default_storage)
        db.session.commit()
        url = url_for('DataSourceUploadApi')
        params = {
            'resumableIdentifier': 'eda-cc0-ffa',
            'resumableFilename': 'new_data_source.csv',
            'resumableChunkNumber': 1,
            'resumableTotalChunks': 1,
            'resumableTotalSize': 20,
            'storage_id': default_storage.id
        }

    data = "id,name\n1,bob\n2,Alice"
    rv = client.post(url, content_type='application/json',
                     data=data,
                     query_string=params,
                     headers={'X-Auth-Token': auth_token})
    assert 200 == rv.status_code, 'Incorrect status code: {}'.format(
        rv.data)


def test_data_source_download_success(client, auth_token, jvm, app, file_ds):
    url = url_for('DataSourceDownload', data_source_id=file_ds.id)

    rv = client.get(url, headers={'X-Auth-Token': auth_token})
    assert 200 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)
    with open(file_ds.url[5:]) as f:
        assert f.read() == rv.data
