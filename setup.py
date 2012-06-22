
import os, sys

from distutils.core import setup
from distutils.command.install_data import install_data
from setuptools import Extension, find_packages

MAJOR_VERSION = '1'
MINOR_VERSION = '0a1'

ext = []
data_files=[]

class compile_m(install_data):
    """
        Compile the mumps source file

        TODO: I need a post-install step to compile the .m file. Otherwise
        the first time this package is run, it must be run as root.
    """
    def run(self):
        install_data.run(self)
        cwd = os.getcwd()
        try:
            source_path = self.install_base + "/vavista/_gtm/"
            os.chdir(source_path)
            cmd = "mumps %s" % "vavistagtm.m"
            print "Compiling GT.M source: ", cmd
            rv = os.system(cmd)
            if rv != 0:
                sys.stderr.write("""
******************************************************************************
Warning: could not compile GT.M mumps file (vavistagtm.m).
----------------------------------------------------------
If you are using GT.M, this file needs to be compiled. 
Check your environment is setup for GT.M and that you can compile a mumps
file, using the command:
    
    mumps filename

If you are running this command under sudo, you may have to set-up the GT.M
environment for root.

I use:

   sudo PATH=/usr/local/gtm:$PATH gtm_dist=$gtm_dist python setup.py install

******************************************************************************
""")
        finally:
            os.chdir(cwd)

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
    cmdclass={"install_data": compile_m},
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

