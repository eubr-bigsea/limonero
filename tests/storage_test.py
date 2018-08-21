# -*- coding: utf-8 -*-
import json


def test_storage_list_fail_not_authorized(client):
    rv = client.get('/storages')
    assert 401 == rv.status_code, 'Incorrect status code'
    resp = json.loads(rv.data)
    assert resp['status'] == 'ERROR', 'Incorrect status'
    assert 'Thorn' in resp['message'], 'Incorrect message'


def test_storage_list_success(client, auth_token, default_storage):
    rv = client.get('/storages', headers={'X-Auth-Token': auth_token})
    assert 200 == rv.status_code, 'Incorrect status code'
    resp = json.loads(rv.data)
    assert len(resp) == 1, 'Wrong quantity'

    assert resp[0]['id'] == default_storage.id
    assert resp[0]['type'] == default_storage.type
    assert resp[0]['url'] == default_storage.url
    assert resp[0]['name'] == default_storage.name
    assert resp[0]['enabled'] == default_storage.enabled


def test_storage_list_many_success(client, auth_token, storages):
    rv = client.get('/storages', headers={'X-Auth-Token': auth_token})
    assert 200 == rv.status_code, 'Incorrect status code'
    resp = json.loads(rv.data)
    assert len(resp) == len(
        [s for s in storages if s.enabled]), 'Wrong quantity'


def test_storage_get_success(client, auth_token, default_storage):
    rv = client.get('/storages/{}'.format(default_storage.id),
                    headers={'X-Auth-Token': auth_token})
    assert 200 == rv.status_code, 'Incorrect status code'
    resp = json.loads(rv.data)
    assert resp['id'] == default_storage.id
    assert resp['type'] == default_storage.type
    assert resp['url'] == default_storage.url
    assert resp['name'] == default_storage.name
    assert resp['enabled'] == default_storage.enabled


def test_storage_not_found_error(client, auth_token):
    rv = client.get('/storages/{}'.format(999),
                    headers={'X-Auth-Token': auth_token})
    assert 404 == rv.status_code, 'Incorrect status code'
