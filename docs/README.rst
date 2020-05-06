
Open Data Cube Docs
===================


Developing Locally
------------------

Requires a Unix like system that includes ``make``.

#. Install NodeJS + NPM
#. Install Browser Sync

.. code-block:: bash

   npm install -g browser-sync

#. Install Python dependencies

.. code-block:: bash

   pip install -r docs-requirements.txt
   pip install git+https://github.com/carrotandcompany/sphinx-autobuild.git@feature_event_delay

#. Start the auto-building development server.

.. code-block:: bash

   sphinx-autobuild &
   browser-sync
