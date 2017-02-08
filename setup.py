"""
setup.py installation file for ``solr-zkutil``

To execute installation run: ``sudo python setup.py install`` from the same
directory as this setup.py file.
"""
import os
from os.path import abspath, dirname, join
import sys

__application__ = 'solr-zkutil'

if abspath(os.getcwd()) != abspath(dirname(__file__)):
    sys.stderr.write("\nNOTICE! You should run this command from the directory that setup.py is in!\n")

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils import setup

REQUIRED_PYTHON_VERSION = (2, 7, 0)
README_PATH = abspath(join(os.path.dirname(__file__), 'README.rst'))
try:
    LONG_DESCRIPTION = open(README_PATH).read()
except Exception as e:
    sys.stderr.write("Couldn't load README.rst - %s" % e)
    LONG_DESCRIPTION = ""

# Python Version Check
if sys.version_info < REQUIRED_PYTHON_VERSION:
    sys.exit(
        '%s requires Python %s or greater' % (__application__, REQUIRED_PYTHON_VERSION.join('.'))
    )
    
if os.name == 'nt':
    SCRIPTS = ['./bin/solr-zkutil.bat', './bin/solr-zkutil.py']
else:
    SCRIPTS = ['./bin/solr-zkutil']

setup(
    name=__application__,
    zip_safe=True,  # ok to compress the source archive on disk?
    version='0.82',
    author='Ben DeMott',
    author_email='ben.demott@gmail.com',
    packages=find_packages(),
    url='https://github.com/bendemott/solr-zkutil',
    license='MIT',
    description='command-line utility for Solr Cloud to show pertinent information in ZooKeeper quickly.',
    long_description=LONG_DESCRIPTION,
    install_requires=[
        'colorama',        # Console colors
        'kazoo',           # ZooKeeper api
    ],
    keywords = ['solr', 'zookeeper', 'cli'], # arbitrary keywords
    scripts=SCRIPTS
)
