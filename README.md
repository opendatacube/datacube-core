# gdf
General Data Framework (GDF)

The scope and design objectives of the GDF can be described as follows:
. The current GDF implementation is intended as a working prototype for a cohesive, sustainable framework for large-scale multidimensional data management for geoscientific data. The design of the GDF has been driven by the longer-term strategic goals of Geoscience Australia (GA) in that it both aligns us with existing best practice in scientific data, and allows us to push well beyond the capabilities of the current implementation of the AGDC.
. It should be noted that the GDF is purely a High Performance Data (HPD) implementation which supports but does not implement any analytical or processing functionality per-se. From its inception, one of its primary objectives has been to provide a means to advance the next generation of the AGDC by enabling greater efficiencies and interoperability.
. The GDF was designed from the outset to support not only the current AGDC Earth Observation and land-use use cases at hand, but also other cross-domain gridded-data use cases by implementing the following features:
	o Flexibility through parameterised management, permitting tuning for specific infrastructure and/or access modes
	o Efficient subsetting in the temporal dimension or across any other dimension(s), including time-series functionality by exploiting HDF5/netCDF4 chunking
	o Support for seamless integration with 3D geophysical, atmospheric and marine data
	o Support for generalised dimensionality up to the limitations of the storage format (<=1024 for netCDF). Dimensionality not limited to XYZT may include spectral band or model run.
	o Efficient handling of sparse and irregular observational data (i.e. not just contiguous and regular model output)
	o Support for a federated system of multiple data collections (with independent security)
	o Support for new, independent, ad-hoc data collections for specific purposes (interim results, subsets, etc)
	o Seamless interoperability between multiple sensors, potentially obviating the need to force data into a single nested gridding scheme
	o Centralised management of metadata providing complete cataloguing of storage unit contents. This permits rapid and accurate planning of execution strategies prior to opening any data files.
. While implementing the prototype gridded data management portion of the GDF, methods for the future integration of non-tiled, irregular point-cloud and vector data into the model are also being investigated and planned. These extensions to the GDF will be pursued more actively in the next financial year.
. The GDF will manage the translation of individual domain-specific data products into consolidated, standards-compliant, self-describing multidimensional storage structures in a sustainable framework. This will allow GA to both align itself with existing best practice in large-scale science domains (notably climate and marine science), as well as providing highly efficient access to seamless subsets of data directly from the multidimensional storage structures via a custom declarative API. It is this declarative API which is intended to support future AGDC analytics.
. The GDF will provide a sustainable means to maintain dynamic data collections by permitting incremental updates and implementing versioning and rollback of all data ingestions (subject to storage capacity limitations). The high-level incremental ingestion workflow to achieve this within the GDF has already been designed and documented and is ready to be implemented.
. The GDF is being developed as part of the HPD R&D program within the Geoinformatics and Data Services Section (GDSS), and its design is consistent with both the Observations & Measurements standard and the GA Enterprise Data Platform model.
