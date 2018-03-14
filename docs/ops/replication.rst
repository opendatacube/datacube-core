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

It can be run from the command line as :command:`datacube-simple-replica`, taking an
optional parameter of a configuration file.

Provide a configuration file in :file:`~/.datacube.replication.conf` in YAML format,
or specify an alternate location on the command line.


Command line documentation
==========================

.. click:: datacube_apps.simple_replica:replicate
   :prog: datacube-simple-replica



Caveats and limitations
=======================

- Remote datacube files and database are accessed via an SSH host that can be
  logged into without a password, ie. by using local SSH key agent.
- The remote datacube index must be same version as the local datacube code.


