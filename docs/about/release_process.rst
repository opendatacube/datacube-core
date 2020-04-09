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

    .. code::

       git tag datacube-1.6.0
       git push --tags

#. Draft a new release on the `Datacube releases`_ GitHub page
    Include the items added to the release notes in step 2.


Marking it stable
=================

Once a built version has been tested, found to be stable, and the team agrees, we make it the new 
stable version.

#. Merge changes leading up to the release into the `stable` branch
    This will also update the `stable` docs.

#. Upload the build to PyPi
    This step is done by GitHub Actions when tag is pushed.

    Manually it looks something like this:

    .. code-block:: bash

        python setup.py sdist bdist_wheel
        twine upload dist/*
        
    This should upload the project to https://pypi.python.org/pypi/datacube/.

#. Update conda-forge recipe
    Follow the instrucions under **Updating datacube-feedstock** in the `Datacube Feedstock`_ repository.
    
    It should involve modifying the version number in the
    `recipe <https://github.com/conda-forge/datacube-feedstock/blob/master/recipe/meta.yaml>`_ and updating the SHA hash.    
    The hash should be generated from the ``.tar.gz`` mentioned in the ``source`` of the recipe.
    
    .. code-block:: bash
    
        openssl sha256 <downloaded-datacube-source.tar.gz>
        
    
.. _PyPI: https://pypi.python.org/pypi
.. _Travis: https://travis-ci.org/opendatacube/datacube-core
.. _readthedocs: http://readthedocs.org/projects/datacube-core/builds/
.. _Datacube releases: https://github.com/opendatacube/datacube-core/releases
.. _Datacube Feedstock: https://github.com/conda-forge/datacube-feedstock

