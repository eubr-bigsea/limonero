import datetime
import json
import os
from urllib.parse import urlparse

from flask import url_for
from flask_babel import gettext
import pytest

from limonero.models import DataSource, DataSourceFormat, db
from limonero.schema import generate_download_token


def test_data_source_list_fail_not_authorized(client, app):
    with app.test_request_context():
        url = url_for('DataSourceListApi')
    rv = client.get(url)
    assert 401 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)
    resp = json.loads(rv.data)
    assert resp['status'] == 'ERROR', 'Incorrect status'
    assert 'Thorn' in resp['message'], 'Incorrect message'


def test_data_source_list_success(client, app):
    with app.test_request_context():
        default_ds = DataSource.query.get(1)

    rv = client.get(
        '/datasources', headers={'X-Auth-Token': str(client.secret)})
    assert 200 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)
    resp = json.loads(rv.data)
    assert len(resp['data']) == 2, f"Wrong quantity: {len(resp['data'])}"

    assert resp['data'][0]['id'] == default_ds.id
    assert resp['data'][0]['format'] == default_ds.format
    assert resp['data'][0]['url'] == default_ds.url
    assert resp['data'][0]['name'] == default_ds.name
    assert resp['data'][0]['enabled'] == default_ds.enabled


def test_data_source_list_many_success(client, app):
    rv = client.get(
        '/datasources', headers={'X-Auth-Token': str(client.secret)})
    assert 200 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)
    resp = json.loads(rv.data)
    with app.test_request_context():
        total = DataSource.query.count()
    assert resp['pagination']['total'] == total, 'Wrong quantity'


def test_data_source_get_success(client, app):
    with app.test_request_context():
        default_ds = DataSource.query.get(1)

    rv = client.get('/datasources/{}'.format(default_ds.id),
                    headers={'X-Auth-Token': str(client.secret)})

    assert 200 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)
    resp = json.loads(rv.data)
    assert resp['id'] == default_ds.id
    assert resp['format'] == default_ds.format
    assert resp['url'] == default_ds.url
    assert resp['name'] == default_ds.name
    assert resp['enabled'] == default_ds.enabled


def test_data_source_not_found_error(client):
    rv = client.get('/datasources/{}'.format(999),
                    headers={'X-Auth-Token': str(client.secret)})
    assert 404 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)


def test_data_source_get_simple_success(client):
    rv = client.get('/datasources',
                    query_string={'simple': 'true'},
                    headers={'X-Auth-Token': str(client.secret)})

    assert 200 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)
    resp = json.loads(rv.data)['data'][0]
    set1 = set(resp.keys())
    set2 = set(['user_id', 'description', 'created', 'download_token', 'format',
                           'privacy_aware', 'user_name', 'id', 'name'])
    assert set1 == set2, set1-set2


def test_data_source_get_fields_success(client):
    rv = client.get('/datasources',
                    query_string={'fields': 'name,id'},
                    headers={'X-Auth-Token': str(client.secret)})

    assert 200 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)
    resp = json.loads(rv.data)['data'][0]
    assert set(resp.keys()) == set(['id', 'name'])


def test_data_source_get_fields_invalid_sort_success(client):
    rv = client.get('/datasources',
                    query_string={'fields': 'name,id', 'sort': 'wrong',
                                  'asc': 'false'},
                    headers={'X-Auth-Token': str(client.secret)})

    assert 200 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)
    resp = json.loads(rv.data)['data'][0]
    assert set(resp.keys()) == set(['id', 'name'])


def test_data_source_simple_list_success(client):
    rv = client.get('/datasources',
                    query_string={'list': 'true'},
                    headers={'X-Auth-Token': str(client.secret)})

    assert 200 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)
    resp = json.loads(rv.data)[0]
    assert set(resp.keys()) == set(['id', 'name'])


def test_data_source_non_existing_field_fail(client):
    rv = client.get('/datasources',
                    query_string={'fields': 'non-existing'},
                    headers={'X-Auth-Token': str(client.secret)})

    assert 500 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)
    resp = json.loads(rv.data)
    assert resp['status'] == 'ERROR'


def test_create_data_source_success(client, app):
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
                     headers={'X-Auth-Token': str(client.secret)})

    assert 200 == rv.status_code, \
        f'Incorrect status code: {rv.status_code} ({rv.json})'
    resp = rv.json['data']
    with app.test_request_context():
        ds = DataSource.query.get(resp['id'])
        assert ds is not None, 'Data source was not created'


def test_delete_data_source_success(client, app):
    with app.test_request_context():
        ds = DataSource(name='To be deleted',
                        description='Optional data source',
                        enabled=False,
                        statistics_process_counter=0,
                        read_only=False,
                        privacy_aware=False,
                        url='hdfs://test2:9000/db/test2',
                        created=datetime.datetime.utcnow(),
                        updated=datetime.datetime.utcnow(),
                        format=DataSourceFormat.PARQUET,
                        provenience=None,
                        estimated_rows=0,
                        estimated_size_in_mega_bytes=0,
                        expiration=None,
                        user_id=1,
                        user_login='lemonade',
                        user_name='Lemonade project',
                        tags=None,
                        temporary=False,
                        workflow_id=None,
                        task_id=None,
                        attribute_delimiter=',',
                        text_delimiter='"',
                        is_public=True,
                        treat_as_missing='NA',
                        encoding='UTF8',
                        is_first_line_header=True,
                        command='',
                        is_multiline=False,
                        storage_id=1)
        db.session.add(ds)
        db.session.commit()
        ds_id = ds.id

    rv = client.delete('/datasources/{}'.format(ds_id),
                       headers={'X-Auth-Token': str(client.secret)})

    assert 200 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)
    resp = json.loads(rv.data)
    assert resp['status'] == 'OK'
    assert resp['message'] == gettext("%(what)s was successfuly deleted",
                                      what=gettext('Data source'))


def test_update_data_source_success(client, app):
    with app.test_request_context():
        default_ds = DataSource.query.get(1)

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
                      headers={'X-Auth-Token': str(client.secret)})

    assert 200 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)

    resp = json.loads(rv.data)
    assert resp['status'] == 'OK'
    assert resp['message'] == gettext("%(what)s was successfuly updated",
                                      what=gettext('Data source'))
    ds = DataSource.query.get(default_ds.id)
    for k, v in payload.items():
        assert getattr(ds, k) == v, \
            'Value differ after update: {} != {}'.format(getattr(ds, k), v)


def test_update_data_source_not_found_fail(client):
    payload = {
        'name': 'New name for data source',
        'format': 'CSV',
        'is_public': True,
        'storage_id': 1,
        'url': 'hdfs://dev:9000/data/test.parquet'

    }
    data_source_id = 7777
    rv = client.patch(f'/datasources/{data_source_id}',
                      content_type='application/json',
                      data=json.dumps(payload),
                      headers={'X-Auth-Token': str(client.secret)})

    assert 400 == rv.status_code, 'Incorrect status code: {}'.format(rv.data)

    resp = json.loads(rv.data)
    assert resp['status'] == 'ERROR'
    assert(resp['message']) == gettext(
        "%(type)s not found.", type=gettext('Data source'))


@pytest.mark.skip('todo')
def test_data_source_infer_simple_schema_success(client, app,
                                                 infer_ds):
    """ Uses JVM !"""

    for ds, meta in infer_ds:
        url = url_for('DataSourceInferSchemaApi', data_source_id=ds.id)
        rv = client.post(url, content_type='application/json',
                         headers={'X-Auth-Token': str(client.secret)})
        assert 200 == rv.status_code, 'Incorrect status code: {}'.format(
            rv.data)
        with app.test_request_context():
            read_ds = DataSource.query.get(ds.id)
            assert len(meta) == len(read_ds.attributes)
            for i, (name, data_type) in enumerate(meta):
                assert data_type == read_ds.attributes[
                    i].type, '{} x {}'.format(name, read_ds.attributes[i].name)


@pytest.mark.skip('todo')
def test_data_source_check_chunk_dont_exist_success(client, file_storage):
    url = url_for('DataSourceUploadApi')
    params = {
        'resumableIdentifier': 'eda-cc0-ffa',
        'resumableFilename': 'new_data_source.csv',
        'resumableChunkNumber': 1,
        'storage_id': file_storage.id
    }
    rv = client.get(url, content_type='application/json',
                    query_string=params,
                    headers={'X-Auth-Token': str(client.secret)})
    assert 404 == rv.status_code, 'Incorrect status code: {}'.format(
        rv.data)


@pytest.mark.skip('todo')
def test_data_source_upload_chunk_success(client, app,
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
                     headers={'X-Auth-Token': str(client.secret)})
    assert 200 == rv.status_code, f'Incorrect status code: {rv.data}'
    with app.test_request_context():
        ds = DataSource.query.get(1)
        assert os.path.exists(ds.url[5:])
        with open(ds.url[5:]) as f:
            assert f.read() == data
        os.unlink(ds.url[5:])


@pytest.mark.skip('todo')
def test_data_source_upload_chunk_hdfs_success(
        client, app, default_storage, hdfs, db):
    cluster, uri, fs = hdfs
    with app.test_request_context():
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
                     headers={'X-Auth-Token': str(client.secret)})
    assert 200 == rv.status_code, f'Incorrect status code: {rv.data}'

@pytest.mark.skip('todo: fix "Popped wrong context"')
def test_data_source_download_success(client, app, datasources):
    with app.test_request_context():
        file_ds =  DataSource.query.get(7004) #next(d for d in datasources if d.url.startswith('file://'))
        token = generate_download_token(file_ds.id)
        url = url_for('DataSourceDownload', data_source_id=file_ds.id,
                      token=token)
    
    rv = client.get(url, headers={'X-Auth-Token': str(client.secret)})
    
    assert 200 == rv.status_code, f'Incorrect status code: {rv.data}'
    assert 'attachment; filename=iris.parquet.csv' == rv.headers.get(
        'Content-disposition')
        #parsed = urlparse(file_ds.url)
        #with open(parsed.path, 'rb') as f:
        #    d = f.read()
        #    assert d == rv.data

