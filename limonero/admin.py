from flask_admin.contrib.sqla import ModelView

from models import Attribute


class DataSourceModelView(ModelView):
    extra_css = [
        '/static/css/site.css'
    ]
    can_delete = True  # disable model deletion
    page_size = 50  # the number of entries to display on the list view
    column_exclude_list = ['description', 'read_only', 'url', 'created',
                           'updated', 'format', 'provenience', 'estimated_rows',
                           'estimated_size_in_mega_bytes', 'expiration',
                           'user_name', 'tags', 'temporary', 'user_id',
                           'task_id', 'workflow_id',
                           'estimated_size_in_mega_bytes', ]
    column_editable_list = ['name']
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


class StorageModelView(ModelView):
    can_delete = False  # disable model deletion
    page_size = 50  # the number of entries to display on the list view
    column_editable_list = ['name']
