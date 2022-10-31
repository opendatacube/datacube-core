Release Process
***************

#. Decide to do a release, and check with regular contributors on Slack that
   they don't have anything pending.

#. Update the release notes in the ``develop`` branch via a PR.

#. Create a new **Tag** and **Release** using the `GitHub Releases Web UI`_

#. Wait for the `GitHub Action`_ to run and publish the new release to PyPI_

#. Wait for the **conda-forge** bot to notice the new PyPI version and create a PR against
   `the conda-forge datacube feedstock <https://github.com/conda-forge/datacube-feedstock/pulls>`_

#. Merge the `PR created by the conda-forge <https://github.com/conda-forge/datacube-feedstock/pulls>`_ bot to create a
   new `conda-forge release <https://anaconda.org/conda-forge/datacube>`_.

#. Manually update the ``stable`` branch via a PR from ``develop``.

#. Kick back, relax, and enjoy a tasty beverage.

.. _GitHub Releases Web UI: https://github.com/opendatacube/datacube-core/releases
.. _GitHub Action: https://github.com/opendatacube/datacube-core/actions
.. _PyPI: https://pypi.org/project/datacube/
