=======================
solr-zkutil CLI Utility 
=======================

About
-----
``solr-zkutil`` is an easy to use tool written in ``Python`` that allows you to quickly understand
information about your **Solr** ZooKeeper cluster.

If you live in an ethereal environment in which solr-hosts are frequently changing, or you have
many different solr environments to keep track of, this tool is for you!

The program is designed to work with **Windows** or **Linux** hosts, and is easy to install.

Runs on **python3** or **python2**!

Features
--------
|livenodesgif|

Supports environment aliases for ZooKeeper Connections
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ 
|environmentconf|

Add your ZooKeeper connection string to the program and an environment alias
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
::

    solr-zkutil config --add 'DEV=zk01.host.com:2181,zk02.host.com:2181,zk03.host.com:2181'
    
| `Or replace the configuration entirely using valid json`

::
    
    solr-zkutil config --configuration '{\"DEV\":\"zk01.dev.host.com:2181,zk02.dev.host.com:2181,zk03.dev.host.com:2181\", \"QA\": \"zk01.qa.host.com:2181,zk02.qa.host.com:2181,zk03.qa.host.com:2181\"}'


|environmentadd|

Query ZooKeeper for Solr Hosts, and Open the administration web-interface automatically
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
| ``solr-zkutil live-nodes --env DEV --browser``

| `or`

| ``solr-zkutil live-nodes -z zk01.dev.host.com:2181 --browser``

|livenodes|

Watch any ZooKeeper file/node for changes, during deployments, etc
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
| ``solr-zkutil watch /live_nodes --env DEV``

| `or`

| ``solr-zkutil watch /clusterstate.json --env PROD`` 

|watchnode|

Issue Administrative Commands Easily
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
| ``solr-zkutil admin RUOK --env DEV``

|admincmd|

| ``solr-zkutil ls /live_nodes -z zk01.dev.host.com:2181,zk02.dev.host.com:2181,zk03.dev.host.com:2181``

| `or.. note that ls can also be used to view the contents of node`

| ``solr-zkutil ls /clusterstate.json --env PROD --all-hosts``

View the contents/children of a node across all ensemble (cluster) members quickly
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
|lsnodesall|

View all nodes, with stats info
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
|lsnodesdebug|

Execute admin commands across all nodes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
|adminall|

Installation
------------

Windows
^^^^^^^
On windows install ``Python 2.7`` or ``Python 3.3+``, and ensure Python ``exe`` and ``Scripts`` paths are configured 
to be on your Windows Path environment, and the program should be accessible through ``PowerShell``
or ``cmd.exe``.  

Setting up Python Paths is an option during the installation of the Python Package on Windows, I highly suggest you check the box, or mark the feature **add Python to system path** so this step is accomplished for you.

`Note: I will try to add a bundled exe installer soon for Windows`

**Python Include Paths**

- ``C:\Python27``
- ``C:\Python27\Scripts``

The ``pip`` command is bundled with the Python Windows installation.

Once you have this configured you should be able to also execute ``pip`` without specifying its path.

Linux
^^^^^
A recent version of Python ships with most linux distributions, if you don't have pip
install it with (ubuntu/debian)::

    sudo apt-get install python-pip
    
(Fedora)::

    sudo dnf install python-pip
    
There are no additional special instructions on Linux.

Installing from PyPi (recommended)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
::

    pip install solr-zkutil
    
Or to update your solr-zkutil to the latest version::

    pip install solr-zkutil --upgrade

Install Manually
^^^^^^^^^^^^^^^^
Simply download, clone the repository, open a console and execute:
``python setup.py install`` 

On windows if you have not configured your Python environment you can install with::

    C:\Python27\python.exe setup.py install

Install from pip/github
^^^^^^^^^^^^^^^^^^^^^^^
::

    pip install git+https://github.com/bendemott/solr-zkutil.git


Program Commands
----------------
Once installed the program is executable using the command::

    solr-zkutil

For help with the command type::

    solr-zkutil --help

If you are having trouble configuring paths the program can also be ran using:: 

    python -m solrzkutil

or for `Windows`:: 

    C:\Python27\python.exe -m solrzkutil 

Usage
^^^^^
::

    usage: solr-zkutil [-h]
                       {live-nodes,clusterstate,watch,ls,stat,admin,config} ...

    positional arguments:
      {live-nodes,clusterstate,watch,ls,stat,admin,config}
                            --- available sub-commands ---
        live-nodes          List Solr Live Nodes from ZooKeeper
        clusterstate        List Solr Collections and Nodes
        watch               Watch a ZooKeeper Node for Changes
        ls                  List a ZooKeeper Node
        stat                Check ZooKeeper ensemble status
        admin               Execute a ZooKeeper administrative command
        config              Show connection strings, or set environment
                            configuration

    optional arguments:
      -h, --help            show this help message and exit
      
      
.. |livenodesgif| image:: http://i.imgur.com/uTROF7Z.gif
.. |environmentconf| image:: http://i.imgur.com/v1df7K9.png
.. |environmentadd| image:: http://i.imgur.com/UL1peUD.png
.. |livenodes| image:: http://i.imgur.com/QpQt1Xs.png
.. |watchnode| image:: http://i.imgur.com/9S9x9wb.png
.. |admincmd| image:: http://i.imgur.com/Wm1DpmL.png
.. |lsnodesall| image:: http://i.imgur.com/Lk1yVtB.png
.. |lsnodesdebug| image:: http://i.imgur.com/mcl54I5.png
.. |adminall| image:: http://i.imgur.com/aDqCdzE.png
