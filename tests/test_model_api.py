# -*- coding: utf-8 -*-
from limonero.models import *
import datetime
from flask import current_app

def test_model_fail_not_authorized(client):
    tests = [
        lambda: client.get('/models'),
        lambda: client.post('/models'),
        lambda: client.get('/models/1'),
        lambda: client.patch('/models/1'),
        lambda: client.delete('/models/1'),
    ]
    for i, test in enumerate(tests):
        rv = test()
        assert 401 == rv.status_code, \
            f'Test {i}: Incorrect status code: {rv.status_code}'
        resp = rv.json
        assert resp['status'] == 'ERROR', f'Test {i}: Incorrect status'
        assert 'Thorn' in resp['message'], f'Test {i}: Incorrect message'


def test_model_list_success(client):
    headers={'X-Auth-Token': str(client.secret)}
    rv = client.get('/models', headers=headers)
    assert 200 == rv.status_code, \
        f'Test model list: Incorrect status code: {rv.status_code}'
    resp = rv.json
    
    with current_app.app_context():
        default_model = Model.query.order_by(
            Model.name).first()
    
    assert resp['data'][0]['id'] == default_model.id
    assert resp['data'][0]['name'] == default_model.name
    assert resp['data'][0]['enabled'] == default_model.enabled
    assert resp['data'][0]['path'] == default_model.path
    assert resp['data'][0]['class_name'] == default_model.class_name
    assert resp['data'][0]['user_id'] == default_model.user_id
    assert resp['data'][0]['user_login'] == default_model.user_login
    assert resp['data'][0]['user_name'] == default_model.user_name

def test_model_list_with_parameters_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    params = {'enabled': 'true', 'fields': 'id, name',
              'asc': 'false', 'query': 'model', 'sort': 'class_name'}

    rv = client.get('/models', headers=headers, query_string=params)
    assert 200 == rv.status_code, 'Incorrect status code'
    resp = rv.json


def test_model_post_missing_data(client):
    headers = {'X-Auth-Token': str(client.secret)}
    params = {}

    rv = client.post('/models', headers=headers, json=params)
    assert 400 == rv.status_code, 'Incorrect status code'
    resp = rv.json
    assert resp['status'] == 'ERROR', 'Wrong status'


def test_model_post_success(client):
    headers = {'X-Auth-Token': str(client.secret)}

    with current_app.app_context():
        default_storage = Storage.query.order_by(
            Storage.id).first()

    params = {'name': 'Model #test', 'enabled': 'true', 'path': '/tmp/model/model_test.zip',
              'class_name': 'LinearRegressionModel', 'type': 'SPARK_ML_CLASSIFICATION', 'user_id': '1',
              'user_name': 'Lemonade project', 'user_login': 'lemonade', 'storage_id': default_storage.id,
              'job_id': '0', 'task_id': '', 'workflow_id': '0', 'workflow_name': ''}
    
    rv = client.post('/models', headers=headers, content_type='application/json', data=json.dumps(params))
    assert 200 == rv.status_code, 'Incorrect status code'
    resp = rv.json


def test_model_get_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    model_id = 1
    rv = client.get(f'/models/{model_id}', headers=headers)
    assert 200 == rv.status_code, 'Incorrect status code'

    with current_app.app_context():
        default_model = Model.query.get(1)
    resp = rv.json
    assert resp['data'][0]['id'] == default_model.id
    assert resp['data'][0]['name'] == default_model.name
    assert resp['data'][0]['enabled'] == default_model.enabled
    assert resp['data'][0]['path'] == default_model.path
    assert resp['data'][0]['class_name'] == default_model.class_name
    assert resp['data'][0]['user_id'] == default_model.user_id
    assert resp['data'][0]['user_login'] == default_model.user_login
    assert resp['data'][0]['user_name'] == default_model.user_name


def test_model_fail_not_found_error(client):
    headers = {'X-Auth-Token': str(client.secret)}
    model_id = 999
    rv = client.get(f'/models/{model_id}', headers=headers)
    assert 404 == rv.status_code, f'Incorrect status code: {rv.status_code}'


def test_model_delete_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    model_id = 9999

    rv = client.get(f'/models/{model_id}', headers=headers)
    assert rv.status_code == 404

    with current_app.app_context():
        model = Model(
            id=model_id, name='Deleted', path='/tmp/model/model_test.zip', 
            class_name='LinearRegressionModel', type= 'SPARK_ML_CLASSIFICATION', user_id= '1',
            user_name= 'Lemonade project', user_login= 'lemonade', storage_id= 1,
            job_id='0', task_id='', workflow_id='0', workflow_name=''
            )
        db.session.add(model)
        db.session.commit()

    rv = client.delete(f'/models/{model_id}', headers=headers)
    assert 200 == rv.status_code, f'Incorrect status code: {rv.status_code}'


def test_model_patch_success(client, app):
    headers = {'X-Auth-Token': str(client.secret)}
    model_id = 8888

    with app.app_context():
        model = Model(
            id=model_id, name='Deleted', path='/tmp/model/model_test.zip', 
            class_name='LinearRegressionModel', type= 'SPARK_ML_CLASSIFICATION', user_id= '1',
            user_name='Lemonade project', user_login='lemonade', storage_id=1,
            job_id='0', task_id='', workflow_id='0', workflow_name=''
            )
        db.session.add(model)
        db.session.commit()

    update = {'type': 'KERAS', 'name': 'Fixed'}
    rv = client.patch(f'/models/{model_id}', json=update, headers=headers)
    assert rv.status_code == 200, rv.text

    with app.app_context():
        model = Model.query.get(model_id)
        assert model.name == update['name']
        assert model.type == update['type']