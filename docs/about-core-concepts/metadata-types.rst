Metadata Types
==============

.. literalinclude:: ../config_samples/metadata_types/bare_bone.yaml
   :language: yaml

.. note::

    Metadata type yaml file must contain name, description and dataset keys.

    Dataset key must contain id, sources, creation_dt, label, and search_fields keys.

    For metadata types of spatial datasets, the dataset key must also contain grid_spatial, measurements, and format keys.
    Support for non-spatial datasets is likely to be dropped in version 2.0.
