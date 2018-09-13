=======================
Miniconda (recommended)
=======================

Install Miniconda
-----------------

Follow conda installation guide for your platform: https://conda.io/docs/install/quick.html

Configure Miniconda
-------------------

Add conda-forge channel

.. code::

    conda config --add channels conda-forge

conda-forge channel provides multitude of community maintained packages.
Find out more about it here https://conda-forge.org/

Create the environment
----------------------

.. code::

    conda create --name cubeenv python=3.6 datacube

Activate the environment on **Linux** and **OS X**

.. code::

    source activate cubeenv

Activate the environment on **Windows**

.. code::

    activate cubeenv

Find out more about managing virtual environments here https://conda.io/docs/using/envs.html


Install other packages
----------------------

.. code::

    conda install jupyter matplotlib scipy

Find out more about managing packages here https://conda.io/docs/using/pkgs.html
