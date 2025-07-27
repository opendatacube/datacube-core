Metadata Types
**************

A Metadata Type defines which fields should be searchable in your product or dataset metadata.

The ``postgres`` index driver has three default metadata types: ``eo``, ``telemetry`` and ``eo3``.

The ``postgis`` driver has only one default metadata type: ``eo3``.

You would create a new metadata type if you want custom fields to be searchable for your products, or
if you want to structure your metadata documents differently.

To add or alter metadata types, you can use commands like: ``datacube metadata add <path-to-file>``
and to update: ``datacube metadata update <path-to-file>``. Using ``--allow-unsafe`` will allow
you to update metadata types where the changes may have unexpected consequences.

Note that the postgis driver only supports `eo3-compatible metadata types`_, and from version 2.0 onward,
support for non-eo3-compatible metadata types will be fully deprecated.

.. literalinclude:: ../config_samples/metadata_types/bare_bone.yaml
   :language: yaml

.. note::

    Metadata type yaml file must contain name, description and dataset keys.

    Dataset key must contain id, sources, creation_dt, label, and search_fields keys.

    For metadata types of spatial datasets, the dataset key must also contain grid_spatial, measurements, and format keys.
    Support for non-spatial datasets is likely to be dropped or changed in non-backwards compatible ways
    in version 2.0.

.. _`eo3-compatible metadata types`: https://github.com/opendatacube/eo3/blob/develop/SPECIFICATION-odc-type.md
