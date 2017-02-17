from flask_migrate import Migrate, MigrateCommand
from flask_script import Manager

from limonero.app import app
from limonero.models import db

migrate = Migrate(app, db)
manager = Manager(app)

manager.add_command('db', MigrateCommand)


@manager.command
def hello():
    print "hello"


if __name__ == "__main__":
    manager.run()
