import flask_login as login
from flask import redirect, url_for, request, current_app, session
from flask_admin import AdminIndexView, expose, helpers
from flask_admin.contrib.sqla import ModelView
from flask_admin.menu import MenuLink
from wtforms import form, fields, validators

from limonero.app_auth import CONFIG_KEY
from models import Attribute


class User(object):
    def __init__(self, user_login, is_admin, password, is_active,
                 is_authenticated=False, user_id=0):
        self.is_admin = is_admin
        self.password = password
        self.is_active = is_active
        self.is_authenticated = is_authenticated
        self.user_id = user_id
        self.user_login = user_login

    def get_id(self):
        return self.user_id


# Initialize flask-login
def init_login(app):
    login_manager = login.LoginManager()
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(user_id):
        return User(**session.get('user'))


# Define login and registration forms (for flask-login)
class LoginForm(form.Form):
    login = fields.StringField(validators=[validators.required()])
    password = fields.PasswordField(validators=[validators.required()])

    def validate_login(self, field):
        user = self.get_user()

        if user is None:
            raise validators.ValidationError('Invalid user')

        if user.password != self.password.data or 'admin' != self.login.data:
            raise validators.ValidationError('Invalid login or password')
        user.is_authenticated = True
        session['user'] = vars(user)

    def get_user(self):
        return User(user_login=self.login.data, is_admin=True, is_active=True,
                    password=current_app.config[CONFIG_KEY]['secret'],
                    is_authenticated=True, user_id=1)


# noinspection PyMethodMayBeStatic
class AccessControlMixin(object):
    def is_accessible(self):
        return login.current_user.is_authenticated


class DataSourceModelView(AccessControlMixin, ModelView):
    extra_css = [
        '/static/css/site.css'
    ]
    can_view_details = True
    can_delete = False  # disable model deletion
    page_size = 50  # the number of entries to display on the list view

    column_list = ['name', 'enabled', 'user_login']
    form_excluded_columns = ['read_only', 'provenience', 'estimated_rows',
                             'estimated_size_in_mega_bytes', 'expiration',
                             'temporary', 'workflow_id', 'created', 'task_id',
                             'estimated_size_in_mega_bytes', ]

    inline_models = [
        (Attribute, dict(form_excluded_columns=[
            'distinct_values', 'mean_value', 'media_value', 'max_value',
            'min_value', 'median_value', 'std_deviation', 'missing_total',
            'deciles', 'missing_representation'
        ]))
    ]


class AuthenticatedMenuLink(AccessControlMixin, MenuLink):
    pass


class HomeView(AdminIndexView):
    @expose('/')
    def index(self):
        if not login.current_user.is_authenticated:
            return redirect(url_for('.login_view'))
        # return super(HomeView, self).index()
        return self.render('admin/home.html')

    @expose('/login/', methods=('GET', 'POST'))
    def login_view(self):
        # handle user login
        login_form = LoginForm(request.form)
        if helpers.validate_form_on_submit(login_form):
            user = login_form.get_user()
            login.login_user(user)

        if login.current_user.is_authenticated:
            return redirect(url_for('.index'))

        self._template_args['form'] = login_form
        # self._template_args['link'] = link
        # return super(HomeView, self).index()
        return self.render('admin/login.html')

    @expose('/logout/')
    def logout_view(self):
        login.logout_user()
        return redirect(url_for('.index'))


class StorageModelView(AccessControlMixin, ModelView):
    can_delete = False  # disable model deletion
    page_size = 50  # the number of entries to display on the list view
    column_filters = ['name']
    # column_editable_list = ['name']
    extra_css = [
        '/static/css/site.css'
    ]
