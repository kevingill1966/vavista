
from distutils.core import setup
from setuptools import find_packages

MAJOR_VERSION = '1'
MINOR_VERSION = '0a1'

data_files=[]

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
    ''',
    namespace_packages=['vavista'],
    packages=find_packages('src'),
    package_dir={'': 'src'},
    scripts = ["src/vavista/scripts/filemand"],
    include_package_data=True,
    test_suite = "vavista.tests",
    zip_safe=False)

