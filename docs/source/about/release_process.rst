Release Process
***************

#. Decide to do a release, and check with regular contributors on `Discord <https://discord.com/invite/4hhBQVas5U>`_
   that they don't have anything pending.

#. Ensure version pins in pyproject.toml and conda-environment.yml are in sync and up to date,
   and refresh uv.lock:

.. code-block:: bash

   uv lock -U

#. Create a new **Tag** and **Draft Release** using the `GitHub Releases Web UI`_

#. Based on the generated release notes from GitHub, update the release notes in
   about/whats_new.rst and the fallback version number in pyproject.toml in the
   ``develop`` branch via a PR.

#. Change the Draft Release to Published using the `GitHub Releases Web UI`_

#. Wait for the `GitHub Action`_ to run and publish the new release to PyPI_

#. Wait for the **conda-forge** bot to notice the new PyPI version and create a PR against
   `the conda-forge datacube feedstock <https://github.com/conda-forge/datacube-feedstock/pulls>`_

#. Fix any errors and merge the
   `PR created by the conda-forge <https://github.com/conda-forge/datacube-feedstock/pulls>`_ bot to create a
   new `conda-forge release <https://anaconda.org/conda-forge/datacube>`_.

#. Manually update the ``stable`` branch:

   - git checkout <release tag>
   - git push --force origin stable

#. Post release announcements on Slack, Discord, and social media platforms.

#. Kick back, relax, and enjoy a tasty beverage.

.. _GitHub Releases Web UI: https://github.com/opendatacube/datacube-core/releases
.. _GitHub Action: https://github.com/opendatacube/datacube-core/actions
.. _PyPI: https://pypi.org/project/datacube/
