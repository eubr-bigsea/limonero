# -*- coding: utf-8 -*-
from limonero.models import *
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
    headers = {'X-Auth-Token': str(client.secret)}
    rv = client.get('/models', headers=headers)
    assert 200 == rv.status_code, 'Incorrect status code'
    resp = rv.json
    assert resp['pagination']['total'] == 2, 'Wrong quantity'

    with current_app.app_context():
        default_model = Model.query.order_by(Model.id).first()

    assert resp['data'][0]['id'] == default_model.id
    assert resp['data'][0]['type'] == default_model.type
    assert resp['data'][0]['path'] == default_model.path
    assert resp['data'][0]['name'] == default_model.name
    assert resp['data'][0]['enabled'] == default_model.enabled


def test_model_list_with_parameters_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    params = {'enabled': 'true', 'fields': 'id,name',
            'asc': 'false', 'query': 'model', 'sort': 'created', 'page': '1'}

    rv = client.get('/models', headers=headers, query_string=params)
    assert 200 == rv.status_code, 'Incorrect status code'
    resp = rv.json
    assert resp['pagination']['total'] == 3, 'Wrong quantity'

def test_model_list_with_parameters_enabled_filter_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    params = {'enabled': 'false', 'fields': 'id,name,enabled',
            'asc': 'false', 'query': 'model', 'sort': 'created', 'page': '1'}

    rv = client.get('/models', headers=headers, query_string=params)
    assert 200 == rv.status_code, 'Incorrect status code'
    resp = rv.json
    assert all([not s['enabled'] for s in resp.get('data')])



def test_model_list_no_page_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    params = {'page': 'false'}

    rv = client.get('/models', headers=headers, query_string=params)
    resp = rv.json
    assert len(resp['data']) == 6, 'Wrong quantity'
    assert 200 == rv.status_code, 'Incorrect status code'


def test_model_post_missing_data(client):
    headers = {'X-Auth-Token': str(client.secret)}
    params = {}

    rv = client.post('/models', headers=headers, json=params)
    assert 400 == rv.status_code, 'Incorrect status code'
    resp = rv.json
    assert resp['status'] == 'ERROR', 'Wrong status'


def test_model_post_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    params = {'path': '/server-hdfs/test',
              'name': 'Test model', 'type': 'HDFS'}

    rv = client.post('/models', headers=headers, json=params)
    assert 200 == rv.status_code, 'Incorrect status code'
    resp = rv.json
    # assert resp['status'] == 'OK', 'Wrong status'


def test_model_get_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    model_id = 1
    rv = client.get(f'/models/{model_id}', headers=headers)
    assert 200 == rv.status_code, 'Incorrect status code'

    with current_app.app_context():
        default_model = Model.query.get(1)
    resp = rv.json
    assert resp['data'][0]['id'] == default_model.id
    assert resp['data'][0]['type'] == default_model.type
    assert resp['data'][0]['path'] == default_model.path
    assert resp['data'][0]['name'] == default_model.name
    assert resp['data'][0]['enabled'] == default_model.enabled


def test_model_fail_not_found_error(client):
    headers = {'X-Auth-Token': str(client.secret)}
    model_id = 999
    rv = client.get(f'/models/{model_id}', headers=headers)
    assert 404 == rv.status_code, f'Incorrect status code: {rv.status_code}'


def test_model_delete_success(client):
    headers = {'X-Auth-Token': str(client.secret)}
    model_id = 8888

    rv = client.get(f'/models/{model_id}', headers=headers)
    assert rv.status_code == 404

    with current_app.app_context():
        model = Model(id=model_id,
            name = "Model #999",
            enabled = True,
            created = datetime.datetime.now(),
            path = '/tmp/model/model1.zip',
            class_name = 'LinearRegressionModel',
            type = ModelType.SPARK_ML_CLASSIFICATION,
            deployment_status = DeploymentStatus.NOT_DEPLOYED,
            user_id = 1,
            user_login = 'admin@lemonade.org.br',
            user_name = 'Admin',
            workflow_id = 0,
            workflow_name = None,
            task_id = '',
            job_id = 0,
            storage_id=1)
        db.session.add(model)
        db.session.commit()

    rv = client.delete(f'/models/{model_id}', headers=headers)
    assert 204 == rv.status_code, f'Incorrect status code: {rv.status_code}'

    with current_app.app_context():
        model = Model.query.get(model_id)
        assert not model.enabled

def test_model_patch_success(client, app):
    headers = {'X-Auth-Token': str(client.secret)}
    model_id = 1111

    with app.app_context():
        model = Model(id=model_id,
            name = "Model #999",
            enabled = True,
            created = datetime.datetime.now(),
            path = '/tmp/model/model1.zip',
            class_name = 'LinearRegressionModel',
            type = ModelType.SPARK_ML_CLASSIFICATION,
            deployment_status = DeploymentStatus.NOT_DEPLOYED,
            user_id = 1,
            user_login = 'admin@lemonade.org.br',
            user_name = 'Admin',
            workflow_id = 0,
            workflow_name = None,
            task_id = '',
            job_id = 0,
            storage_id=1)
        db.session.add(model)
        db.session.commit()

    update = {'path': '/data/classification.tgz', 'name': 'Fixed'}
    rv = client.patch(f'/models/{model_id}', json=update, headers=headers)
    assert rv.status_code == 200

    with app.app_context():
        model = Model.query.get(model_id)
        assert model.name == update['name']
        assert model.path == update['path']

def test_model_patch_not_found_error(client, app):
    headers = {'X-Auth-Token': str(client.secret)}
    model_id = 1000

    update = {'path': '/data/classification.tgz', 'name': 'Fixed'}
    rv = client.patch(f'/models/{model_id}', json=update, headers=headers)

    assert rv.status_code == 404
