.. _release_process:

Build a version
===============

#. Pick a release name for the next version
    Releases are versioned using the ``major.minor.bugfix`` numbering system.

#. Update the release notes on the :ref:`whats_new` page
    Check the git log for changes since the last release.

#. Check that Travis_ and readthedocs_ are passing for the latest commit
    Make sure that the tests have finished running!

#. Tag the branch
    Use the format of ``datacube-major.minor.bugfix``.

#. Draft a new release on the Datacube_ GitHub repository
    Include the items added to the release notes in step 2.

#. Mark the version as released in Jira_
    Move any open issues to the next version.

#. Install the datacube module on `raijin`
    Follow the instructions on installing the **Data Cube** module on the `Datacube Environment`_ repository,
    but do not yet make it the default module version.

Marking it stable
=================

Once/if a built version has been tested on Raijin, found to be stable, and the team agrees, we make it the new default
stable version.

#. Merge changes leading up to the release into the `stable` branch
    This will also update the `stable` docs

#. Upload the build to PyPi.

    .. code-block:: bash
    
        python setup.py sdist bdist_wheel
        twine upload dist/*

#. Update the default version on `raijin`
    Follow the instructions under **Update default version** in the `Datacube Environment`_ repository

#. Notify the community of the release using the Datacube Central mailing list
    Ask Simon Oliver for the MailChimp details.


.. _Travis: https://travis-ci.org/opendatacube/datacube-core

.. _readthedocs: http://readthedocs.org/projects/datacube-core/builds/

.. _Datacube: https://github.com/opendatacube/datacube-core/releases

.. _Jira: https://gaautobots.atlassian.net/projects/ACDD?selectedItem=com.atlassian.jira.jira-projects-plugin%3Arelease-page&status=unreleased

.. _Datacube Environment: https://github.com/GeoscienceAustralia/ga-datacube-env#data-cube-module
