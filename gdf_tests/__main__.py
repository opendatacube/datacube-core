from __future__ import absolute_import
from . import test_arguments
from . import test_config_file
from . import test_database
from . import test_gdf

# Run all tests
test_arguments.main()
test_config_file.main()
test_database.main()
test_gdf.main()
