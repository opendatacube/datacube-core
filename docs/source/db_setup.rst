AGDC Database Setup
===================

Setup local DB server
---------------------

Add standard groups & users and create new, empty DB::

    cd gdf_database
    ./gdf_db_setup.sh

Restore a PostgreSQL DB backup to a new DB
------------------------------------------

Restoring database back-up::

    cd gdf_database
    ./create_db_from_backup.sh <new_db_name> <db_backup_filename>

.. note::
    # Make sure <code_root_dir>/gdf_default.conf is edited to refer to new DB
