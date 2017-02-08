======================
solr-zkutil CLI Utilty 
======================

About
-----
``solr-zkutil`` is an easy to use tool written in ``Python`` that allows you to quickly understand
information about your **Solr** ZooKeeper cluster.

If you live in an ethereal environment in which solr-hosts are frequently changing, or you have
many different solr environments to keep track of, this tool is for you!

The program is designed to work with Windows or Linux hosts, and is easy to install.

Features
--------

Host/Environment Configurations
````````````````````````````````
**Supports environment aliases for ZooKeeper Connections**
|environmentconf|

**Add your ZooKeeper connection string to the program and an environment alias**
|environmentadd|

**Query ZooKeeper for Solr Hosts, and Open the administration web-interface automatically**
|livenodes|

**Watch any ZooKeeper file/node for changes, during deployments, etc**
|watchnode|

**Issue Administrative Commands Easily**
|admincmd|

**View the contents/children of a node across all ensemble (cluster) members quickly**
|lsnodesall|

Installation
------------

Windows
```````
On windows install ``Python 2.7``, and ensure Python 2.7 ``exe`` and ``Scripts`` paths are configured 
to be on your Windows Path environment, and the program should be accessible through ``PowerShell``
or ``cmd.exe``

**Python Include Paths**
- ``C:\Python27``
- ``C:\Python27\Scripts``

Once you have this configured you should be able to also execute ``pip`` without specify it's path.

Install Manually
````````````````
Simply download, clone the repository, open a console and execute:
``python setup.py install`` 

On windows if you have not configured your Python environment you can install with::

    C:\Python27\python.exe setup.py install

Install from pip/github
```````````````````````
``pip install git+https://github.com/bendemott/solr-zkutil.git``

Installing from PyPi
````````````````````
``pip install solr-zkutil``

Program Commands
----------------
Once installed the program is executable using the command ``solr-zkutil``, for help with the command
type ``solr-zkutil --help``

If you are having trouble configuring paths the program can also be ran using ``python -m zksolrutil``
or ``C:\Python27\python.exe -m zksolrutil`` in `Windows`

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
      

.. |environmentconf| image:: http://i.imgur.com/v1df7K9.png
.. |environmentadd| image:: http://i.imgur.com/UL1peUD.png
.. |livenodes| image:: http://i.imgur.com/QpQt1Xs.png
.. |watchnode| image:: http://i.imgur.com/9S9x9wb.png
.. |admincmd| image:: http://i.imgur.com/Wm1DpmL.png
.. |lsnodesall| image:: http://i.imgur.com/yz33NXI.png