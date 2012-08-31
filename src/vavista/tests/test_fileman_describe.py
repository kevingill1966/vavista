
import unittest
import re

from vavista.fileman import connect
from vavista.fileman.dbsdd import (FT_DATETIME, FT_NUMERIC, FT_SET, FT_TEXT,
    FT_WP, FT_COMPUTED, FT_POINTER, FT_VPOINTER, FT_MUMPS, FT_SUBFILE)

class TestDescribe(unittest.TestCase):
    """
        I want to test this against a real table
    """
    _queue = None

    def setUp(self):
        self.dbs = connect("0", "")

    def test_race(self):
        self._queue = []

        pytest = self.dbs.get_file("NEW PERSON")
        desc = pytest.fm_description
        self.output_model('NEW PERSON', desc)

        # check self.queue for embedded models that need to be output
        while len(self._queue) > 0:
            name, desc = self._queue.pop()
            self.output_model(name, desc)

    def output_model(self, name, description):
        """
            Print out the model spec for one table
        """
        print '\n'
        print "class %s(models.FMModel):" % name
        print '    """'
        if description["description"]:
            for row in description['description'].split('\n'):
                print "        ", row
            print "\n"
            print "        Fileid:", description['fileid']
        print '    """'
        for field in description['fields']:
            field_type, field_params, field_notes = self.field_info(field)
            if field_notes:
                comments = " # " + ", ".join(field_notes)
            else:
                comments = ""
            params = []
            for k, v in sorted(field_params.items()):
                params.append("%s=%s" % (k, repr(v)))
            params = ", ".join(params)
            print "    %s = %s(%s)%s%s" % (
                    field['name'], field_type, params,
                    field_type.find("(") > -1 and ")" or "",
                    comments)
        print '\n'

    def field_info(self, field):
        """
            Returns:
            field_type - class for the field.
            field_params - key, value pairs to be passed to the field constructor
            field_notes - text to be placed into comments beside the field
        """
        if field['fmql_type'] == FT_TEXT:
            field_type, field_params, field_notes = self.field_info_text(field)
        elif field['fmql_type'] == FT_SET:
            field_type, field_params, field_notes = self.field_info_set(field)
        elif field['fmql_type'] == FT_DATETIME:
            field_type, field_params, field_notes = self.field_info_date(field)
        elif field['fmql_type'] == FT_SUBFILE:
            # Subfile can be a LIST or an EMBEDDED Model
            if len(field['children']['fields']) == 1:
                # LIST
                field_type, field_params, field_notes = self.field_info_list(field)
            else:
                field_type, field_params, field_notes = self.field_info_embedded_model(field)
        else:
            field_type = 'UNKNOWN'
            field_params = {}
            field_notes = []

        # Handle generic concepts, e.g. mandatory
        if not field['mandatory']:
            field_params['blank'] = True
            field_params['null'] = True
        if field['m_valid']:
            field_params['m_valid'] = field['m_valid']  # the m_validation - what can I do with this?
        if field['fieldhelp']:
            field_params['help_text'] = field['fieldhelp']
        if field['fieldhelp2']:
            field_params['help_text2'] = field['fieldhelp2']
        if field['title']:
            field_params['title'] = field['title']
        else:
            field_params['title'] = field['fieldinfo'][0]

        return field_type, field_params, field_notes

    def field_info_text(self, field):
        field_type = 'model.FMTextField'
        field_params = {}
        field_notes = []

        # work out the max-length
        mm = self.max_min_len(field['m_valid'])
        if mm:
            field_params['min_length'] = int(mm[0])
            field_params['max_length'] = int(mm[1])
        else:
            field_params['max_length'] = 240 # todo - what is the max really?

        return field_type, field_params, field_notes

    def max_min_len(self, m_valid):
        # attempt to work out the field max/min length from the vista
        # field validation code
        # e.g. 'K:$L(X)>1!($L(X)<1) X'

        if m_valid:
            m = re.search(r"K\:\$L\(X\)\>(?P<max_length>\d+)\!\(\$L\(X\)\<(?P<min_length>\d+)\) X", m_valid)
            if m:
                return m.group('min_length'), m.group('max_length')
        return None

    def date_format(self, m_valid):
        # Attempt to work out the date format from the M validation code.
        # 'S %DT="EX" D ^%DT S X=Y K:X<1 X'
        # here the code is checking for format X, which is full date.
        # See fileman programmers guide, section 2.3.55

        if m_valid:
            m = re.search(r'S \%DT="(?P<format>[^"]*)"', m_valid)
            if m:
                format = m.group('format')
                return ''.join([f for f in format if f not in ['A', 'E']]) # A, E are both interaction, not validation
        return None

    def field_info_set(self, field):
        field_type = 'model.FMChoiceField'
        field_params = {}
        field_notes = []

        # What are the choices?
        field_params['choices'] = field['details']

        return field_type, field_params, field_notes

    def field_info_date(self, field):
        field_type = 'model.FMDateTimeField'
        field_params = {}
        field_notes = []

        # What are the choices?
        field_params['format'] = self.date_format(field['m_valid'])

        return field_type, field_params, field_notes

    def field_info_list(self, field):
        field_type = 'model.FMListField'
        field_params = {}
        field_notes = []

        return field_type, field_params, field_notes

    def field_info_embedded_model(self, field):
        field_type = 'model.FMListField(model.FMEmbeddedModelField'
        field_params = {}
        field_notes = []

        # need to push out the embedded model separately.
        sf_name = "SF%s" % field['subfileid'].replace(".", "_")
        self._queue.append((sf_name, field['children']))
        field_params = {'name': sf_name}

        return field_type, field_params, field_notes


test_cases = (TestDescribe, )

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite
