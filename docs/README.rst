
Open Data Cube Docs
===================


Developing Locally
------------------

Requires a Unix like system that includes ``make``.

#. Clone the datacube-core repository. If you don't have permissions to push to the datacube-core library, you will need to fork the repository and clone your fork.

.. code-block:: bash

   git clone https://github.com/opendatacube/datacube-core.git

#. Check out a new branch for the documentation feature you're working on

.. code-block:: bash

   git switch -c docs-<feature>

#. Change directory to the docs folder

.. code-block:: bash

   cd docs

#. Create a conda environment for python 3.11, with conda-forge as the channel

.. code-block:: bash

   conda create --name datacubecoredocs -c conda-forge python=3.11

#. Activate the conda environment

.. code-block:: bash

   conda activate datacubecoredocs

#. Install pandoc

.. code-block:: bash

   conda install pandoc

#. Install requirements with pip

.. code-block:: bash

   pip install -r requirements.txt

#. Run the autobuild.

.. code-block:: bash

   sphinx-autobuild . _build/html

#. Open a browser and navigate to the URL provided by the autobuild

#. Make changes to the docs. The terminal with the autobuild will continue to update the docs view in the browser.

#. When finished, quit the autobuild process using ``ctrl-c`` in the terminal.

#. Stage and commit your changes.

#. When ready for review, push your changes and create a pull request.
