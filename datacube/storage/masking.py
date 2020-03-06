import warnings

warnings.warn("datacube.storage.masking has moved to datacube.utils.masking",
              category=DeprecationWarning)

from datacube.utils.masking import *
