#
# TODO: I need a post-install step to compile the .m file. Otherwise
# the first time this package is run, it must be run as root.

from setuptools import setup, Extension, find_packages

MAJOR_VERSION = '1'
MINOR_VERSION = '0a1'

ext = []
data_files=[]

_gtm = Extension('vavista._gtm',
    define_macros = [('MAJOR_VERSION', MAJOR_VERSION),
                     ('MINOR_VERSION', MINOR_VERSION)],
    include_dirs = ['/usr/include/python2.7',
                    '/usr/local/gtm'],
    libraries = ['gtmshr','python2.7'],
    library_dirs = ['/usr/local/gtm'],
    sources = ['src/vavista/_gtm/_gtm.c'])

ext.append(_gtm)

setup(
    name='vavista',
    version='%s.%s' % (MAJOR_VERSION, MINOR_VERSION),
    author='Kevin Gill',
    author_email='kevin.gill@openapp.ie',
    license="TO BE DETERMINED",
    platforms=["linux"],
    url='http://www.python.org/doc/current/ext/building.html',
    description='VAVista Utilities',
    long_description='''
    This is an attempt to create a useful set of functions to integrate Python with VA Vista.
    This package is intended to run in process with GT.M, i.e. this is not client-server to
    the database like FMQL.
    ''',
    namespace_packages=['vavista'],
    packages=find_packages('src'),
    package_dir={'': 'src'},
    data_files=[('vavista/_gtm', ['src/vavista/_gtm/calltab.ci', 'src/vavista/_gtm/vavistagtm.m']),],
    ext_modules=ext,
    include_package_data=True,
    test_suite = "vavista.tests",
    zip_safe=False)

