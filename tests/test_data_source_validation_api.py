# -*- coding: utf-8 -*-
from limonero.models import *
from flask import current_app

def test_validation_fail_not_authorized(client):
    tests = [
        lambda: client.get('/datasources/validations'),
        lambda: client.post('/datasources/validations/executions'),
        lambda: client.get('/datasources/validations/1'),
        lambda: client.patch('/datasources/validations/executions/1'),
    ]
    for i, test in enumerate(tests):
        rv = test()
        assert 401 == rv.status_code, \
            f'Test {i}: Incorrect status code: {rv.status_code}'
        resp = rv.json
        assert resp['status'] == 'ERROR', f'Test {i}: Incorrect status'
        assert 'Thorn' in resp['message'], f'Test {i}: Incorrect message'

def test_validation_list(client):
    headers={'X-Auth-Token': str(client.secret)}
    rv = client.get('/datasources/validations', headers=headers)
    assert 200 == rv.status_code, \
        f'Test validation list: Incorrect status code: {rv.status_code}'
    resp = rv.json
    
    with current_app.app_context():
        default_validation = DataSourceValidation.query.order_by(
            DataSourceValidation.description).first()
    breakpoint()
    assert resp['data'][0]['id'] == default_validation.id
    assert resp['data'][0]['description'] == default_validation.description
    assert resp['data'][0]['type'] == default_validation.type
    assert resp['data'][0]['enabled'] == default_validation.enabled
    assert resp['data'][0]['user_id'] == default_validation.user_id
    assert resp['data'][0]['user_login'] == default_validation.user_login
    assert resp['data'][0]['user_name'] == default_validation.user_name