# -*- coding: utf-8 -*-
from limonero.models import *
from flask import current_app


def test_storage_fail_not_authorized(client):
    tests = [
        lambda: client.get('/storages'),
        lambda: client.post('/storages'),
        lambda: client.get('/storages/1'),
        lambda: client.patch('/storages/1'),
        lambda: client.delete('/storages/1'),
    ]
    for i, test in enumerate(tests):
        rv = test()
        assert 401 == rv.status_code, \
            f'Test {i}: Incorrect status code: {rv.status_code}'
        resp = rv.json
        assert resp['status'] == 'ERROR', f'Test {i}: Incorrect status'
        assert 'Thorn' in resp['message'], f'Test {i}: Incorrect message'


def test_storage_list_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    rv = client.get('/storages', headers=headers)
    assert 200 == rv.status_code, 'Incorrect status code'
    resp = rv.json
    assert resp['pagination']['total'] == 5, 'Wrong quantity'

    with current_app.app_context():
        default_storage = Storage.query.order_by(Storage.name).first()

    assert resp['data'][0]['id'] == default_storage.id
    assert resp['data'][0]['type'] == default_storage.type
    assert resp['data'][0]['url'] == default_storage.url
    assert resp['data'][0]['name'] == default_storage.name
    assert resp['data'][0]['enabled'] == default_storage.enabled


def test_storage_list_with_parameters_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    params = {'enabled': 'true', 'fields': 'id,name',
              'asc': 'false', 'query': 'storage', 'sort': 'created'}

    rv = client.get('/storages', headers=headers, query_string=params)
    assert 200 == rv.status_code, 'Incorrect status code'
    resp = rv.json
    assert resp['pagination']['total'] == 3, 'Wrong quantity'


def test_storage_list_no_page_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    params = {'page': 'false'}

    rv = client.get('/storages', headers=headers, query_string=params)
    assert 200 == rv.status_code, 'Incorrect status code'
    resp = rv.json
    assert len(resp['data']) == 5, 'Wrong quantity'


def test_storage_post_missing_data(client):
    headers = {'X-Auth-Token': str(client.secret)}
    params = {}

    rv = client.post('/storages', headers=headers, json=params)
    assert 400 == rv.status_code, 'Incorrect status code'
    resp = rv.json
    assert resp['status'] == 'ERROR', 'Wrong status'


def test_storage_post_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    params = {'url': 'hdfs://server-hdfs/test',
              'name': 'Test storage', 'type': 'HDFS'}

    rv = client.post('/storages', headers=headers, json=params)
    assert 200 == rv.status_code, 'Incorrect status code'
    resp = rv.json
    # assert resp['status'] == 'OK', 'Wrong status'


def test_storage_get_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    storage_id = 1
    rv = client.get(f'/storages/{storage_id}', headers=headers)
    assert 200 == rv.status_code, 'Incorrect status code'

    with current_app.app_context():
        default_storage = Storage.query.get(1)
    resp = rv.json
    assert resp['data'][0]['id'] == default_storage.id
    assert resp['data'][0]['type'] == default_storage.type
    assert resp['data'][0]['url'] == default_storage.url
    assert resp['data'][0]['name'] == default_storage.name
    assert resp['data'][0]['enabled'] == default_storage.enabled


def test_storage_fail_not_found_error(client):
    headers = {'X-Auth-Token': str(client.secret)}
    storage_id = 999
    rv = client.get(f'/storages/{storage_id}', headers=headers)
    assert 404 == rv.status_code, f'Incorrect status code: {rv.status_code}'


def test_storage_delete_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    storage_id = 9999

    rv = client.get(f'/storages/{storage_id}', headers=headers)
    assert rv.status_code == 404

    with current_app.app_context():
        storage = Storage(
            id=storage_id, url='file:///tmp', type='HDFS', name='Deleted')
        db.session.add(storage)
        db.session.commit()

    rv = client.delete(f'/storages/{storage_id}', headers=headers)
    assert 204 == rv.status_code, f'Incorrect status code: {rv.status_code}'

    with current_app.app_context():
        storage = Storage.query.get(storage_id)
        assert not storage.enabled

def test_storage_patch_success(client, app):
    headers = {'X-Auth-Token': str(client.secret)}
    storage_id = 8888

    with app.app_context():
        storage = Storage(
            id=storage_id, url='file:///tmp', type='HDFS', name='Updated')
        db.session.add(storage)
        db.session.commit()

    update = {'url': 'hdfs://teste.com', 'name': 'Fixed'}
    rv = client.patch(f'/storages/{storage_id}', json=update, headers=headers)
    assert rv.status_code == 200

    with app.app_context():
        storage = Storage.query.get(storage_id)
        assert storage.name == update['name']
        assert storage.url == update['url']