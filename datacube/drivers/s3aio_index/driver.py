
from .index import S3AIOIndex
from datacube.model import MetadataType


class S3IndexDriver(object):
    @staticmethod
    def connect_to_index(config, application_name=None, validate_connection=True):
        return S3AIOIndex.from_config(config, application_name, validate_connection)

    @staticmethod
    def metadata_type_from_doc(definition):
        """
        :param dict definition:
        :rtype: datacube.model.MetadataType
        """
        MetadataType.validate(definition)
        return MetadataType(definition,
                            dataset_search_fields=S3AIOIndex.get_dataset_fields(definition))


def index_driver_init():
    return S3IndexDriver()
