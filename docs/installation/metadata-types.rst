Metadata Types
**************

A Metadata Type defines which fields should be searchable in your product or dataset metadata.

Three metadata types are added by default called ``eo``, ``telemetry`` and ``eo3``.

You can see the default metadata types in the repository at `datacube/index/default-metadata-types.yaml <https://github.com/opendatacube/datacube-core/blob/develop/datacube/index/default-metadata-types.yaml>`_.

You would create a new metadata type if you want custom fields to be searchable for your products, or
if you want to structure your metadata documents differently.

To add or alter metadata types, you can use commands like: ``datacube metadata add <path-to-file>``
and to update: ``datacube metadata update <path-to-file>``. Using ``--allow-unsafe`` will allow
you to update metadata types where the changes may have unexpected consequences.

Note that from version 1.9 onward, only eo3-compatible metadata types will be accepted.

.. literalinclude:: ../config_samples/metadata_types/bare_bone.yaml
   :language: yaml

.. note::

    Metadata type yaml file must contain name, description and dataset keys.

    Dataset key must contain id, sources, grid_spatial, measurements, creation_dt, label, format, and search_fields keys.
