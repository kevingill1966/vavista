
import os, sys
import os.path

from distutils.core import setup
from distutils.command.install_data import install_data
from setuptools import Extension, find_packages

MAJOR_VERSION = '1'
MINOR_VERSION = '0a1'

ext = []
data_files=[]

gtm_dist = os.getenv("gtm_dist")
gtm_ver = os.path.basename(gtm_dist).replace(".", "_").replace("-", "_")

class compile_m(install_data):
    """
        Install the mumps source file

        Don't fail to install if this does not compile. Other parts of 
        the package, such as RPCs will still work.
    """
    def run(self):
        install_data.run(self)
        cwd = os.getcwd()
        if not gtm_dist:
            sys.stderr.write("""
******************************************************************************
Warning: could not install GT.M mumps file (vavistagtm.m).
----------------------------------------------------------
If you are using GT.M, this file needs to be copied to your $gtm_dist folder. 
Check your environment is setup for GT.M.
    
    $ echo $gtm_dist

If you are running this command under sudo, you may have to set-up the GT.M
environment for root.

I use:

   sudo PATH=/usr/local/gtm:$PATH gtm_dist=$gtm_dist python setup.py install

******************************************************************************
""")
            return
        try:
            # Copy the source files to the $gtm_dist folder
            source_path = self.install_base + "/vavista/_gtm/"
            os.chdir(source_path)
            cmd = "mumps %s" % "vavistagtm.m"
            print "Compiling GT.M source: ", cmd
            rv = os.system(cmd)
            if rv == 0:
                rv = os.system('/bin/mv vavistagtm.m vavistagtm.o "%s"' % gtm_dist)
            if rv != 0:
                sys.stderr.write("""
******************************************************************************
Warning: could not copy GT.M mumps file (vavistagtm.m).
----------------------------------------------------------

The mumps file could not be copied to the GT.M distribution folder.
Check that the folder exists and that you have write permissions to that folder.

$gtm_dist = "%s"

******************************************************************************
""" % gtm_dist)
        finally:
            os.chdir(cwd)

_gtm = Extension('vavista.gtm%s._gtm' % gtm_ver,
    define_macros = [('MAJOR_VERSION', MAJOR_VERSION),
                     ('MINOR_VERSION', MINOR_VERSION)],
    include_dirs = ['/usr/include/python2.6',
		    '/opt/fis-gtm/V5.4.002B_x86_64'],
    libraries = ['gtmshr','python2.6', "rt"],
    library_dirs = [gtm_dist],
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

