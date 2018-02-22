.. _release_process:

Release Process
***************

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


Marking it stable
=================

Once/if a built version has been tested on Raijin, found to be stable, and the team agrees, we make it the new default
stable version.

#. Merge changes leading up to the release into the `stable` branch
    This will also update the `stable` docs.

#. Upload the build to PyPi
    You might need a PyPI_ account with appropriate authorization.

    .. code-block:: bash

        python setup.py sdist bdist_wheel
        twine upload dist/*
        
    This should upload the project to https://pypi.python.org/pypi/datacube/.

#. Update conda-forge recipe
    Follow the instrucions under **Updating datacube-feedstock** in the `Datcube Feedstock`_ repository.
    
    It should involve modifying the version number in the
    `recipe <https://github.com/conda-forge/datacube-feedstock/blob/master/recipe/meta.yaml>`_ and updating the SHA hash.    
    The hash should be generated from the ``.tar.gz`` mentioned in the ``source`` of the recipe.
    
    .. code-block:: bash
    
        openssl sha256 <downloaded-datacube-source.tar.gz>
        
    
#. Update the default version on `raijin`
    Follow the instructions under **Update default version** in the `Datacube Environment`_ repository.

#. Notify the community of the release using the Datacube Central mailing list
    The notifications are sent out using MailChimp_. You might need an invitation from the Geoscience
    Australia account.
    
    Create a campaign (possibly by replicating one of the existing ones) and change the notes.
    You could send out a test mail to selected accounts before sending it out to the entire DEA
    Beta Users list.

.. _PyPI: https://pypi.python.org/pypi

.. _Travis: https://travis-ci.org/opendatacube/datacube-core

.. _readthedocs: http://readthedocs.org/projects/datacube-core/builds/

.. _Datacube: https://github.com/opendatacube/datacube-core/releases

.. _Jira: https://gaautobots.atlassian.net/projects/ACDD?selectedItem=com.atlassian.jira.jira-projects-plugin%3Arelease-page&status=unreleased

.. _Datacube Environment: https://github.com/GeoscienceAustralia/digitalearthau/tree/develop/modules

.. _Datcube Feedstock: https://github.com/conda-forge/datacube-feedstock#updating-datacube-feedstock

.. _MailChimp: https://www.mailchimp.com
