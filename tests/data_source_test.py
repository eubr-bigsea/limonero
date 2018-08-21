# -*- coding: utf-8 -*-
import json

from limonero.models import DataSource


def test_data_source_list_fail_not_authorized(client):
    rv = client.get('/datasources')
    assert 401 == rv.status_code, 'Incorrect status code'
    resp = json.loads(rv.data)
    assert resp['status'] == 'ERROR', 'Incorrect status'
    assert 'Thorn' in resp['message'], 'Incorrect message'


def test_data_source_list_success(client, auth_token, default_ds):
    rv = client.get('/datasources', headers={'X-Auth-Token': auth_token})
    assert 200 == rv.status_code, 'Incorrect status code'
    resp = json.loads(rv.data)
    assert len(resp['data']) == 1, 'Wrong quantity'

    assert resp['data'][0]['id'] == default_ds.id
    assert resp['data'][0]['format'] == default_ds.format
    assert resp['data'][0]['url'] == default_ds.url
    assert resp['data'][0]['name'] == default_ds.name
    assert resp['data'][0]['enabled'] == default_ds.enabled


def test_data_source_list_many_success(client, auth_token, datasources):
    rv = client.get('/datasources', headers={'X-Auth-Token': auth_token})
    assert 200 == rv.status_code, 'Incorrect status code'
    resp = json.loads(rv.data)
    assert len(resp['data']) == len(datasources), 'Wrong quantity'


def test_data_source_get_success(client, auth_token, default_ds):
    rv = client.get('/datasources/{}'.format(default_ds.id),
                    headers={'X-Auth-Token': auth_token})

    assert 200 == rv.status_code, 'Incorrect status code'
    resp = json.loads(rv.data)
    assert resp['id'] == default_ds.id
    assert resp['format'] == default_ds.format
    assert resp['url'] == default_ds.url
    assert resp['name'] == default_ds.name
    assert resp['enabled'] == default_ds.enabled


def test_data_source_not_found_error(client, auth_token):
    rv = client.get('/datasources/{}'.format(999),
                    headers={'X-Auth-Token': auth_token})
    assert 404 == rv.status_code, 'Incorrect status code'


def test_data_source_get_simple_success(client, auth_token, default_ds):
    rv = client.get('/datasources',
                    query_string={'simple': 'true'},
                    headers={'X-Auth-Token': auth_token})

    assert 200 == rv.status_code, 'Incorrect status code'
    resp = json.loads(rv.data)['data'][0]
    assert resp.keys() == [u'user_id', u'description', u'created',
                           u'privacy_aware', u'user_name', u'id', u'name']


def test_data_source_get_fields_success(client, auth_token, default_ds):
    rv = client.get('/datasources',
                    query_string={'fields': 'name,id'},
                    headers={'X-Auth-Token': auth_token})

    assert 200 == rv.status_code, 'Incorrect status code'
    resp = json.loads(rv.data)['data'][0]
    assert resp.keys() == [u'id', u'name']


def test_data_source_get_fields_invalid_sort_success(client, auth_token,
                                                     default_ds):
    rv = client.get('/datasources',
                    query_string={'fields': 'name,id', 'sort': 'wrong',
                                  'asc': 'false'},
                    headers={'X-Auth-Token': auth_token})

    assert 200 == rv.status_code, 'Incorrect status code'
    resp = json.loads(rv.data)['data'][0]
    assert resp.keys() == [u'id', u'name']


def test_data_source_simple_list_success(client, auth_token, default_ds):
    rv = client.get('/datasources',
                    query_string={'list': 'true'},
                    headers={'X-Auth-Token': auth_token})

    assert 200 == rv.status_code, 'Incorrect status code'
    resp = json.loads(rv.data)[0]
    assert resp.keys() == [u'id', u'name']


def test_data_source_non_existing_field_fail(client, auth_token, default_ds):
    rv = client.get('/datasources',
                    query_string={'fields': 'non-existing'},
                    headers={'X-Auth-Token': auth_token})

    assert 500 == rv.status_code, 'Incorrect status code'
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

    assert 200 == rv.status_code, 'Incorrect status code'
    resp = json.loads(rv.data)
    with app.app_context():
        ds = DataSource.query.get(resp['id'])
        assert ds is not None, 'Data source was not created'


def test_delete_data_source_success(client, auth_token, app, default_ds):
    rv = client.delete('/datasources/{}'.format(default_ds.id),
                       headers={'X-Auth-Token': auth_token})

    assert 200 == rv.status_code, 'Incorrect status code'
    resp = json.loads(rv.data)
    assert resp['status'] == 'OK'
    assert 'successfuly deleted' in resp['message']
