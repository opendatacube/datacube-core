Metadata Types
==============

.. literalinclude:: ../config_samples/metadata_types/bare_bone.yaml
   :language: yaml

.. note::

    Metadata type yaml file must contain name, description and dataset keys.

    Dataset key must contain id, sources, creation_dt, label, and search_fields keys.

    The dataset key must also contain grid_spatial, measurements, and format keys.

    For a detailed description of the format of a valid metadata type document, refer to the
    `formal specification`_ in the eo3 Github repository.

.. _`formal specification`: https://github.com/opendatacube/eo3/blob/develop/SPECIFICATION-odc-type.md
