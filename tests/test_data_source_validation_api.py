# -*- coding: utf-8 -*-
from limonero.models import *
from flask import current_app

def test_validation_fail_not_authorized(client):
    tests = [
        lambda: client.get('/datasources/validations'),
        lambda: client.post('/datasources/validations'),
        lambda: client.get('/datasources/validations/1'),
        lambda: client.patch('/datasources/validations/1'),
        lambda: client.delete('/datasources/validations/1'),

        lambda: client.get('/datasources/validations/executions'),
        lambda: client.post('/datasources/validations/executions'),
        lambda: client.get('/datasources/validations/executions/1'),
        lambda: client.delete('/datasources/validations/executions/1'),
    ]
    for i, test in enumerate(tests):
        rv = test()
        assert 401 == rv.status_code, \
            f'Test {i}: Incorrect status code: {rv.status_code}'
        resp = rv.json
        assert resp['status'] == 'ERROR', f'Test {i}: Incorrect status'
        assert 'Thorn' in resp['message'], f'Test {i}: Incorrect message'


def test_validation_list_success(client):
    headers={'X-Auth-Token': str(client.secret)}
    rv = client.get('/datasources/validations', headers=headers)
    assert 200 == rv.status_code, \
        f'Test validation list: Incorrect status code: {rv.status_code}'
    resp = rv.json
    
    with current_app.app_context():
        default_validation = DataSourceValidation.query.order_by(
            DataSourceValidation.description).first()
    
    assert resp['data'][0]['id'] == default_validation.id
    assert resp['data'][0]['description'] == default_validation.description
    assert resp['data'][0]['type'] == default_validation.type
    assert resp['data'][0]['enabled'] == default_validation.enabled
    assert resp['data'][0]['user_id'] == default_validation.user_id
    assert resp['data'][0]['user_login'] == default_validation.user_login
    assert resp['data'][0]['user_name'] == default_validation.user_name


def test_validation_list_with_parameters_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    params = {'enabled': 'true', 'fields': 'id, description',
              'asc': 'false', 'query': 'data_source_validation', 'sort': 'type'}

    rv = client.get('/datasources/validations', headers=headers, query_string=params)
    assert 200 == rv.status_code, 'Incorrect status code'
    resp = rv.json


def test_validation_list_no_page_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    params = {'page': 'false'}

    rv = client.get('/storages', headers=headers, query_string=params)
    assert 200 == rv.status_code, 'Incorrect status code'
    resp = rv.json
    assert len(resp['data']) == 5, 'Wrong quantity'


def test_validation_post_missing_data(client):
    headers = {'X-Auth-Token': str(client.secret)}
    params = {}

    rv = client.post('/datasources/validations', headers=headers, json=params)
    assert 400 == rv.status_code, 'Incorrect status code'
    resp = rv.json
    assert resp['status'] == 'ERROR', 'Wrong status'


# This one is not passing (500 Internal Server Error), dont know why
def test_validation_post_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    params = {'description': 'Validação teste', 'type': 'SCRIPT', 'enabled': 'true', 'user_id': '1',
              'user_name': 'Lemonade project', 'user_login': 'lemonade'}

    rv = client.post('/datasources/validations', headers=headers, json=params)
    assert 200 == rv.status_code, 'Incorrect status code'
    resp = rv.json
    assert resp['status'] == 'OK', 'Wrong status'



# Also have to do the validation execution api tests