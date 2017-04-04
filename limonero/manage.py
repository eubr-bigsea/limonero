from flask_migrate import Migrate, MigrateCommand
from flask_script import Manager

from limonero.app import app
from limonero.models import db, DataSource

migrate = Migrate(app, db)
manager = Manager(app)

manager.add_command('db', MigrateCommand)


def required_ok(_, v):
    return v is not None and v.strip() != ''


def in_list_ok(options, v):
    return v in options.get('values', [])


create_database_fields = [
    ["name", {"validation": required_ok, 'label': 'Name (EN):'}],
    ["name_pt", {"validation": required_ok, 'label': 'Name (PT):'}],
    ["description", {"validation": required_ok, 'label': 'Description (EN):'}],
    ["description_pt",
     {"validation": required_ok, 'label': 'Description (PT):'}],
    ["url", {"validation": required_ok, 'label': 'URL:'}],
    ["format",
     {"validation": in_list_ok, 'values': ['TEXT', 'CSV', 'JSON', 'SHAPEFILE'],
      'label': 'Format:',
      'default': 'CSV'}],
    ["storage_id",
     {"validation": required_ok, 'label': 'Storage Id:', 'default': 1}],
]


@manager.command
def update_data_sources_statistics():
    work = DataSource.query.filter(
        DataSource.statistics_process_counter == 0,
        DataSource.id == 4,
    )
    # Data frame: count rows= > df.count(), approx?
    # Size in MB?

    # Attributes
    # from pyspark.sql.functions import col, countDistinct, approxCountDistinct
    # Approx. count of distinct values (OK)
    # df.agg(*(approxCountDistinct(col(c)).alias(c) for c in df.columns)).show()
    #
    # Get numeric attributes:
    # numerics = [x.name for x in df.schema if isinstance(x.dataType, (
    #       types.IntegerType, types.ByteType, types.LongType, types.ShotType
    #       types.FloatType, types.DoubleType, types.DecimalType, ))]
    # Mean
    # import itertools
    # means = df.agg(dict(itertools.product(numerics, ['avg'])))
    #
    # median
    # For large database
    # df.approxQuantile('age', [.5], .25)
    # Small one:
    # ????
    #
    #  Max, min, std_dev
    # min_max_stddev_mean = [functions.max(attr) for attr in attrs] + \
    #       [functions.min(attr) for attr in attrs] + \
    #       [functions.stddev(attr) for attr in attrs] + \
    #       [functions.mean(attr) for attr in attrs] + \
    #       [functions.count(attr) for attr in attrs] + \
    #       [functions.approxCountDistinct(attr) for attr in attrs]
    # df.agg(*min_max_stddev).show()
    #
    # Missing:
    # count = [functions.count(attr) for attr in attrs]
    # df.agg(*count).show()
    # The result is the number of rows in df - count for each column
    # # vertical (column-wise) operations in SQL ignore NULLs

    # histogram
    # df.select('fare').rdd.flatMap(lambda x: x).histogram(10)
    #  URI = spark._jvm.java.net.URI
    #  FileSystem = spark._jvm.org.apache.hadoop.fs.FileSystem
    #  Configuration = spark._jvm.org.apache.hadoop.conf.Configuration
    #  fs = FileSystem.get(URI('hdfs://spark01.ctweb.inweb.org.br:9000'), Configuration())
    #  Path = spark._jvm.org.apache.hadoop.fs.Path
    #  status = fs.globStatus(Path('/lemonade/samples/titanic.csv'))
    # status[0].getLen()

    for ds in work:
        print ds.name


if __name__ == "__main__":
    manager.run()
