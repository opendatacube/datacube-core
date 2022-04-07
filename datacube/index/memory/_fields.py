from typing import Any, Mapping
from datacube.model.fields import SimpleField, Field, get_dataset_fields as generic_get_dataset_fields

# TODO: SimpleFields cannot handle non-metadata fields because e.g. the extract API expects a doc, not a Dataset model
def get_native_fields() -> Mapping[str, Field]:
    return {
        "id": SimpleField(
            ["id"],
            str, 'string',
            name="id",
            description="Dataset UUID"
        ),
        "product": SimpleField(
            ["product", "name"],
            str, 'string',
            name="product",
            description="Product name"
        ),
        "label": SimpleField(
            ["label"],
            str, 'string',
            name="label",
            description=""
        ),
        "format": SimpleField(
            ["format", "name"],
            str, 'string',
            name="format",
            description="File format (GeoTIFF, NetCDF)"
        ),
        "metadata_doc": SimpleField(
            [],
            str, 'string',
            name="metadata_doc",
            description="Full metadata document"
        ),
    }

def get_dataset_fields(metadata_definition: Mapping[str, Any]) -> Mapping[str, Field]:
    fields = get_native_fields()
    fields.update(generic_get_dataset_fields(metadata_definition))
    return fields
