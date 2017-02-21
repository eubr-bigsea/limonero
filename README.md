# limonero
[logo]: docs/img/limonero.png "Lemonade Limonero"

![alt text][logo]

Data source metadata information for Lemonade Project

### Install
```
git clone git@github.com:eubr-bigsea/limonero.git
cd limonero
pip install -r requirements.txt
```

### Config
Copy `limonero.yaml.example` to `limonero.yaml`
```
cp limonero.yaml.example limonero.yaml
```

Create a database named `limonero` in a MySQL server and grant permissions to a user.
```
#Example
mysql -uroot -pmysecret -e "CREATE DATABASE limonero;"
```

Edit `limonero.yaml` according to your database config
```
limonero:
    port: 3321
    environment: prod
    servers:
        database_url: mysql+pymysql://user:secret@server:3306/limonero
    services:
    config:
        SQLALCHEMY_POOL_SIZE: 0
        SQLALCHEMY_POOL_RECYCLE: 60
```
### Run
```
LIMONERO_CONFIG=limonero.yaml python limonero/app.py
```

#### Using docker
Build the container
```
docker build -t bigsea/limonero .
```

Repeat [config](#config) stop and run using config file
```
docker run \
  -v $PWD/limonero.yaml:/usr/src/app/limonero.yaml \
  -p 3321:3321 \
  bigsea/limonero
```
