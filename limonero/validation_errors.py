"""" Module used in order to extract Marshmallow validation errors to pyBabel """
from gettext import gettext

def get_translations():
    return [
            gettext('Missing data for required field.'),
    ]
