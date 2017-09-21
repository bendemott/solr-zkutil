"""
setup.py installation file for ``solr-zkutil``

To execute installation run: ``sudo python setup.py install`` from the same
directory as this setup.py file.


Maintainers Note... Upload to PYPI:
    1.) Update version below to increment number

    2.) Upload to pypi test (to test)
    python setup.py sdist upload -r pypitest
    
    3.) Upload to pypi prod
    python setup.py sdist upload -r pypi
    
Maintainers Note... Tagging:
    
    1.) tag new version with ``git tag -a 0.xx``
    2.) 
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
    try:
        from distutils import setup
    except ImportError:
        pip = "pip"
        if sys.version_info >= (3,0):
            pip = "pip3"
        sys.stderr.write("setuptools is not installed, you can install with: %s install setuptools\n" % pip)
        sys.exit(-1)

README_PATH = abspath(join(os.path.dirname(__file__), 'README.rst'))
try:
    LONG_DESCRIPTION = open(README_PATH).read()
except Exception as e:
    sys.stderr.write("Couldn't load README.rst - %s" % e)
    LONG_DESCRIPTION = ""
    
setup(
    name=__application__,
    zip_safe=True,  # ok to compress the source archive on disk?
    version='0.99',
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
        'six',
        'pendulum',
    ],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    package_data={'solrzkutil': ['bin/solr-zkutil.bat']},
    keywords = ['solr', 'zookeeper', 'cli'], # arbitrary keywords
    scripts=['bin/solr-zkutil.bat', 'bin/solr-zkutil.py', 'bin/solr-zkutil']
)
