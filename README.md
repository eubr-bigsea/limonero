# limonero
Data source metadata information for Lemonade Project

### Install
```
git clone git@github.com:eubr-bigsea/limonero.git
cd limonero
pip install -r requirements.txt
```

### Config
Copy `limonero.json.example` to `limonero.json`
```
cp limonero.json.example limonero.json
```

Create a database named `limonero`
```
#Example
mysql -uroot -pmysecret -e "CREATE DATABASE limonero;"
```

Edit `limonero.json` according to your database config
```
{
  "servers": {
    "database_url": "mysql://root:mysecret@localhost:3306/limonero",
    "environment": "dev"
  }
}
```
### Run
```
python limonero/app_api.py -c limonero.json
```

#### Using docker
Build the container
```
docker build -t bigsea/limonero .
```

Repeat [config](#config) stop and run using config file
```
docker run \
  -v $PWD/limonero.json:/usr/src/app/limonero.json \
  -p 5000:5000 \
  bigsea/limonero
```
