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