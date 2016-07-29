.. highlight:: console

.. _nci_usage_guide:

NCI Usage Guide
===============

.. note::
    These instructions are only relevant to using the :term:`AGDC` on the :term:`NCI` in Australia.

    For details on accessing the NCI, see http://nci.org.au/.

    For details on accessing the :term:`VDI` at NCI, see http://vdi.nci.org.au/help.

To have access to the AGCD modules on ``raijin`` or ``VDI``, run the command::

    [usr111@raijin ~]$ module use /g/data/v10/public/modules/modulefiles/

You should now have access to the following modules::

    [usr111@raijin ~]$ module avail agdc

    ------------------- /g/data/v10/public/modules/modulefiles/ --------------------
    agdc-py2/1.0.2+80.g80bc6aa       agdc-py2-prod/1.0.1
    agdc-py2-demo/0.0.0+974.g39465cf agdc-py2-prod/1.0.2
    agdc-py2-demo/1.0.2+22.g09c5345  agdc-py3-demo/0.0.0+974.g39465cf
    agdc-py2-dev/1.0.2+20.g368a323   agdc-py3-demo/1.0.2+23.g2f27f92
    agdc-py2-dev/1.0.2+23.g2f27f92   agdc-py3-dev/1.0.2+20.g368a323
    agdc-py2-dev/1.0.2+65.g9caf1ca   agdc-py3-dev/1.0.2+23.g2f27f92
    agdc-py2-dev/latest              agdc-py3-dev/1.0.2+65.g9caf1ca
    agdc-py2-env/anaconda2-2.5.0     agdc-py3-env/20160211
    agdc-py2-prod/1.0.0              agdc-py3-env/anaconda3-2.5.0


There are different types of modules:

* **prod** - Production module: real data using a stable codebase.
* **dev** - Development module: with the latest code and features, but not guaranteed to be fully working. Data to change from time to time as needed.
* **demo** - Demonstration module: moderately stable, with sample datasets not necessarily found elsewhere.

Along with 2 versions of Python:

 * **py2** - Python 2.7
 * **py3** - Python 3.5

To load the production module with Python 2, run::

    [usr111@raijin ~]$ module load agdc-py2-prod

The first time you load the module, it will register your account with the database, granting you read-only access.
It will store your password in the file `~/.pgpass`.
This file must have secure permission rights set, so you may need to run::

    chmod 0600 ~/.pgpass

.. note::
    If you are running on VDI, and would also like to use the datacube from raijin,
    you can copy your pgpass file to raijin by running on a terminal window in VDI::

        remote-hpc-cmd init
        ssh raijin "cat >> ~/.pgpass" < ~/.pgpass
        ssh raijin "chmod 0600 ~/.pgpass"

    On VDI, you will be prompted to unlock the private key. Enter your NCI password when prompted.

    If you have previously used the module on raijin, you will need to copy your pgpass file to your VDI home folder.
    From a terminal window in VDI, run::

        ssh raijin "cat ~/.pgpass" >> ~/.pgpass
        chmod 0600 ~/.pgpass

.. warning::
    If you have created a ``.datacube.conf`` file in your home folder from previous versions, you should rename or remove it
    to avoid it conflicting with the settings loaded by the module.
