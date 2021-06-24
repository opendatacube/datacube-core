Introduction
=============================

Sometimes data to load into an Open Data Cube will come packaged with
compatible :ref:`dataset-metadata-doc` and be ready to :ref:`index <indexing>`
immediately.

In other cases you will need to generate these :ref:`dataset-metadata-doc` yourself.
This is done using a ``Dataset Preparation Script``, which reads whatever format metadata
has been supplied with the data, and either writes out ODC compatible files, or adds
records directly to an ODC database.

For common distribution formats there is likely to be a script already, but
other formats will require one to be modified.

.. admonition:: Existing dataset-metadata-docs
   :class: tip

   Examples of prepare scripts are found in the `datacube-dataset-config <https://github.com/opendatacube/datacube-dataset-config>`_ repository
   on Github.

