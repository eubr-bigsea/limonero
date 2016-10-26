FROM python:2.7-onbuild
MAINTAINER Guilherme Maluf <guimalufb@gmail.com>

EXPOSE 5000
CMD [ "python", "./limonero/app_api.py", "-c", "limonero.json" ]
