.. _release_process:

Release Process
===============

When the

1. Pick a release name

2. Update the release notes on the :ref:`whats_new` page with changes since the last release

3. Tag the branch, with the format of ``datacube-x.y.z``, where ``x.y.z`` is the `major`, `minor` and `bugfix` versions

4. Draft a new release on GitHub
   https://github.com/data-cube/agdc-v2/releases

5. Mark the version as released in Jira_, moving open issues to the next version.

.. _Jira: https://gaautobots.atlassian.net/projects/ACDD?selectedItem=com.atlassian.jira.jira-projects-plugin%3Arelease-page&status=unreleased

6. Install the datacube module on `raijin` by following the instructions at
   https://github.com/GeoscienceAustralia/ga-datacube-env#data-cube-module
