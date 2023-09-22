# -*- coding: utf-8 -*-
from limonero.models import *
from flask import current_app

def test_validation_list(client):
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