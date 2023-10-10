# -*- coding: utf-8 -*-
from limonero.models import *
import datetime
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


def test_validation_post_missing_data(client):
    headers = {'X-Auth-Token': str(client.secret)}
    params = {}

    rv = client.post('/datasources/validations', headers=headers, json=params)
    assert 400 == rv.status_code, 'Incorrect status code'
    resp = rv.json
    assert resp['status'] == 'ERROR', 'Wrong status'


def test_validation_post_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    params = {'description': 'Validação teste', 'type': 'SCRIPT', 'enabled': 'true', 'user_id': '1',
              'user_name': 'Lemonade project', 'user_login': 'lemonade', 'data_source_id': 1}

    rv = client.post('/datasources/validations', headers=headers, json=params)
    assert 201 == rv.status_code, 'Incorrect status code'
    resp = rv.json


def test_validation_get_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    validation_id = 1
    rv = client.get(f'/datasources/validations/{validation_id}', headers=headers)
    assert 200 == rv.status_code, 'Incorrect status code'

    with current_app.app_context():
        default_validation = DataSourceValidation.query.get(1)
    resp = rv.json
    assert resp['data'][0]['id'] == default_validation.id
    assert resp['data'][0]['description'] == default_validation.description
    assert resp['data'][0]['type'] == default_validation.type
    assert resp['data'][0]['enabled'] == default_validation.enabled
    assert resp['data'][0]['user_id'] == default_validation.user_id
    assert resp['data'][0]['user_login'] == default_validation.user_login
    assert resp['data'][0]['user_name'] == default_validation.user_name


def test_validation_fail_not_found_error(client):
    headers = {'X-Auth-Token': str(client.secret)}
    validation_id = 999
    rv = client.get(f'/datasources/validations/{validation_id}', headers=headers)
    assert 404 == rv.status_code, f'Incorrect status code: {rv.status_code}'


def test_validation_delete_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    validation_id = 9999

    rv = client.get(f'/datasources/validations/{validation_id}', headers=headers)
    assert rv.status_code == 404

    with current_app.app_context():
        validation = DataSourceValidation(
            id=validation_id, description='Deleted', type='SCRIPT', enabled=True, 
            user_id=1, user_name='Lemonade project', user_login='lemonade', data_source_id=1)
        db.session.add(validation)
        db.session.commit()

    rv = client.delete(f'/datasources/validations/{validation_id}', headers=headers)
    assert 200 == rv.status_code, f'Incorrect status code: {rv.status_code}'


def test_validation_patch_success(client, app):
    headers = {'X-Auth-Token': str(client.secret)}
    validation_id = 8888

    with app.app_context():
        validation = DataSourceValidation(
            id=validation_id, description='Updated', type='SCRIPT', enabled=True, 
            user_id=1, user_name='Lemonade project', user_login='lemonade', data_source_id=1)
        db.session.add(validation)
        db.session.commit()

    update = {'type': 'GREAT_EXPECTATIONS', 'description': 'Fixed'}
    rv = client.patch(f'/datasources/validations/{validation_id}', json=update, headers=headers)
    assert rv.status_code == 200, rv.text

    with app.app_context():
        validation = DataSourceValidation.query.get(validation_id)
        assert validation.description == update['description']
        assert validation.type == update['type']




def test_validation_execution_list_success(client):
    headers={'X-Auth-Token': str(client.secret)}
    rv = client.get('/datasources/validations/executions', headers=headers)
    assert 200 == rv.status_code, \
        f'Test validation execution list: Incorrect status code: {rv.status_code}'
    resp = rv.json
    
    with current_app.app_context():
        default_validation_execution = DataSourceValidationExecution.query.order_by(
            DataSourceValidationExecution.id).first()

    assert resp['data'][0]['id'] == default_validation_execution.id
    assert resp['data'][0]['status'] == default_validation_execution.status
    assert resp['data'][0]['user_id'] == default_validation_execution.user_id
    assert resp['data'][0]['user_login'] == default_validation_execution.user_login
    assert resp['data'][0]['user_name'] == default_validation_execution.user_name
    assert resp['data'][0]['result'] == default_validation_execution.result


def test_validation_execution_list_with_parameters_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    params = {'status': 'SUCCESS', 'fields': 'id, result',
              'asc': 'false', 'query': 'data_source_validation_execution', 'sort': 'created'}

    rv = client.get('/datasources/validations/executions', headers=headers, query_string=params)
    assert 200 == rv.status_code, 'Incorrect status code'
    resp = rv.json


def test_validation_execution_post_missing_data(client):
    headers = {'X-Auth-Token': str(client.secret)}
    params = {}

    rv = client.post('/datasources/validations/executions', headers=headers, json=params)
    assert 400 == rv.status_code, 'Incorrect status code'
    resp = rv.json
    assert resp['status'] == 'ERROR', 'Wrong status'


def test_validation_execution_post_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    params = {'status': 'SUCCESS', 'user_id': '1', 'user_name': 'Lemonade project', 
              'user_login': 'lemonade', 'data_source_validation_id': 1, 'result': 'SUCCESS'}

    rv = client.post('/datasources/validations/executions', headers=headers, json=params)
    assert 201 == rv.status_code, 'Incorrect status code'
    resp = rv.json


def test_validation_execution_get_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    validation_execution_id = 1
    rv = client.get(f'/datasources/validations/executions/{validation_execution_id}', headers=headers)
    assert 200 == rv.status_code, 'Incorrect status code'

    with current_app.app_context():
        default_validation_execution = DataSourceValidationExecution.query.get(1)
    resp = rv.json
    assert resp['data'][0]['id'] == default_validation_execution.id
    assert resp['data'][0]['status'] == default_validation_execution.status
    assert resp['data'][0]['user_id'] == default_validation_execution.user_id
    assert resp['data'][0]['user_login'] == default_validation_execution.user_login
    assert resp['data'][0]['user_name'] == default_validation_execution.user_name
    assert resp['data'][0]['result'] == default_validation_execution.result


def test_validation_execution_fail_not_found_error(client):
    headers = {'X-Auth-Token': str(client.secret)}
    validation_execution_id = 999
    rv = client.get(f'/datasources/validations/executions/{validation_execution_id}', headers=headers)
    assert 404 == rv.status_code, f'Incorrect status code: {rv.status_code}'


def test_validation_execution_delete_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    validation_execution_id = 9999

    rv = client.get(f'/datasources/validations/executions/{validation_execution_id}', headers=headers)
    assert rv.status_code == 404

    with current_app.app_context():
        validation_execution = DataSourceValidationExecution(
            id=validation_execution_id, status='SUCCESS', result='ERROR', user_id=1, 
            user_name='Lemonade project', user_login='lemonade', data_source_validation_id=1)
        db.session.add(validation_execution)
        db.session.commit()

    rv = client.delete(f'/datasources/validations/executions/{validation_execution_id}', headers=headers)
    assert 200 == rv.status_code, f'Incorrect status code: {rv.status_code}'