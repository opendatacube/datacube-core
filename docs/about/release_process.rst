.. _release_process:

Release Process
===============

1. Pick a release name for the next version.
    Releases are version using the ``major.minor.bugfix`` numbering system.

2. Update the release notes on the :ref:`whats_new` page.
    Check the git log for changes since the last release.

3. Tag the branch.
    Use the format of ``datacube-x.y.z``.

4. Draft a new release on the Datacube_ GitHub repository.
    Include the items added to the release notes in setp 2.

.. _Datacube: https://github.com/data-cube/agdc-v2/releases

5. Mark the version as released in Jira_.
    Move any open issues to the next version.

.. _Jira: https://gaautobots.atlassian.net/projects/ACDD?selectedItem=com.atlassian.jira.jira-projects-plugin%3Arelease-page&status=unreleased

6. Install the datacube module on `raijin`.
    Follow the instructions on installing the Data Cube module on the `Datacube Environment`_ repository.

.. _Datacube Environment: https://github.com/GeoscienceAustralia/ga-datacube-env#data-cube-module

7. Notify the community of the release using the Datacube Central mailing list.
    Ask Simon Oliver for the MailChimp details.
