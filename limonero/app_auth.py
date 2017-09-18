# -*- coding: utf-8 -*-}
import json
from collections import namedtuple
from functools import wraps

import re
import requests
from flask import request, Response, current_app, g as flask_g

User = namedtuple("User", "id, login, email, first_name, last_name, locale")

MSG1 = 'Could not verify your access level for that URL. ' \
       'You have to login with proper credentials provided by Lemonade Thorn'

MSG2 = 'Could not verify your access level for that URL. ' \
       'Invalid authentication token'

CONFIG_KEY = 'LIMONERO_CONFIG'


def authenticate(msg, params):
    """Sends a 401 response that enables basic auth"""
    return Response(json.dumps({'status': 'ERROR', 'message': msg}), 401,
                    mimetype="application/json")


def requires_auth(f):
    @wraps(f)
    def decorated(*_args, **kwargs):
        config = current_app.config[CONFIG_KEY]
        internal_token = request.args.get(
            'token', request.headers.get('x-auth-token'))
        authorization = request.headers.get('authorization')
        user_id = request.headers.get('x-user-id')

        if authorization:
            expr = re.compile(r'Token token="(.+?)", email="(.+)?"')
            token, email = expr.findall(authorization)[0]
            # It is using Thorn
            url = '{}/api/tokens'.format(config['services']['thorn']['url'])
            payload = json.dumps({
                'data': {
                    'attributes': {
                        'authenticity-token': token,
                        'email': email
                    },
                    'type': 'tokens',
                    'id': str(user_id)
                }
            })
            headers = {
                'content-type': "application/json",
                'authorization': authorization,
                'cache-control': "no-cache",
            }
            r = requests.request("POST", url, data=payload,
                                 headers=headers)

            if r.status_code != 200:
                return authenticate(MSG2, {})
            else:
                import pdb
                pdb.set_trace()
                user_data = json.loads(r.text)
                setattr(flask_g, 'user', User(id=user_data['id'],
                                              login=user_data['uid'],
                                              email=user_data['email'],
                                              first_name=user_data['firstname'],
                                              last_name=user_data['lastname'],
                                              locale=user_data['locale']))
                return f(*_args, **kwargs)
        elif internal_token:
            if internal_token == str(config['secret']):
                # System user being used
                setattr(flask_g, 'user', User(0, '', '', '', '', ''))
                return f(*_args, **kwargs)
            else:
                return authenticate(MSG2, {"message": "Invalid X-Auth-Token"})
        else:
            return authenticate(MSG1, {'message': 'Invalid authentication'})

    return decorated
