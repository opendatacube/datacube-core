AGDC Database Setup
===================

.. attention::

    Run these script as a valid postgres user.

    You must have a properly configured postgres installation for this to work. If you have a fresh install of postgres on Ubuntu then you will need to configure the postgres user password to `complete the postgres setup <https://help.ubuntu.com/community/PostgreSQL>`_


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

    Make sure <code_root_dir>/gdf_default.conf is edited to refer to new DB

    If this database has been restored from a different system you will need to update any storage locations that have changed in the public.storage_type table.

