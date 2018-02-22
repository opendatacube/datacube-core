.. _replication:

Data Replication
****************

Simple Data Cube Replication Tool
=================================

This tool provides a very simplistic way to download data and metadata from a
remote Data Cube onto a local PC. It connects to a remote Data Cube via SSH,
and downloads database records and files.

A configuration file is used to define which portions of which Product should
be downloaded. If a Dataset is already available locally, it will not be
downlaoded again, meaning the tool can be run multiple times to keep the local
system up to date with new datasets on the remote server.

It can be run from the command line as :ref:`datacube-simple-replica`, taking an
optional parameter of a configuration file.

Provide a configuration file in :ref:`datacube-replication-config` in YAML format,
or specify an alternate location on the command line.


Configuration
=============

As an example, the following configration will download 3 Products for the
specified time and space range. Queries are specified using the same
terms as for the Data Cube Query API.


.. code-block:: yaml
   :caption: ~/.datacube.replication.conf
   :name: datacube-replication-config

    remote_host: raijin.nci.org.au
    remote_user: example12345
    db_password: xxxxxxxxxxxx

    remote_dir: /g/data/
    local_dir: C:/datacube/

    replicated_data:
    - product: ls5_pq_albers
      crs: EPSG:3577
      x: [1200000, 1300000]
      y: [-4200000, -4300000]
      time: [2008-01-01, 2010-01-01]

    - product: ls7_pq_albers
      crs: EPSG:3577
      x: [1200000, 1300000]
      y: [-4200000, -4300000]
      time: [2008-01-01, 2010-01-01]

    - product: ls8_pq_albers
      crs: EPSG:3577
      x: [1200000, 1300000]
      y: [-4200000, -4300000]
      time: [2008-01-01, 2010-01-01]

Caveats and limitations
=======================

- Remote datacube files and database are accessed via an SSH host that can be
  logged into without a password, ie. by using local SSH key agent.
- The remote datacube index must be same version as the local datacube code.


Command line documentation
==========================

.. _datacube-simple-replica:

.. datacube:click-help:: datacube-simple-replica



