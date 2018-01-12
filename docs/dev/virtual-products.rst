===============================
Datacube Virtual Product Design
===============================

Background
----------
Many common use cases of Datacube (DC) involve combining temporally and spatially similar sets of data to produce output data. An example is using Product Quality (PQ) products with other products to perform cloud masking. Issues have been encountered with dataset merging previously in Datacube stats (see https://gist.github.com/Kirill888/a4b52d0077fa4e36b351c22827782492) and in Datacube WMS there are times where only TIRS sensor datasets are available, causing unexpected results. As such the creation of Virtual Products (VPs) has been considered as a way to provide users a way to easily define common patterns used in Datacube work and to solve issues seen in software which uses Datacube surrounding the merging and transformation of data without having to create bespoke code each time.


Products in Datacube
--------------------
In the current version of DC (1.5.4), products are unique strings which associate that string (the product name) with metadata about Datasets. This metadata includes information such as the measurements that the product contains, the grid specification and the mapping of raw fields in the dataset to computed fields (e.g. from_dt and to_dt which product a time measurement). In DC the products are represented by a `DatasetType` which has a `MetadataType`. Each `Dataset` has a `DatasetType` and the `DatasetType` and `MetadataType` are typically loaded from JSON documents stored in a database. See also (http://damien-agdc.readthedocs.io/en/extradocs/dev/data_model.html)

Virtual Products
----------------
Requirements (in progress)
~~~~~~~~~~~~~~~~~~~~~~~~~~

- Metadata as bands, e.g. data about what LandSat data came from may be encoded as metadata which will be preserved as a band
- Timestamps must be retained
- Must support DC GridWorkflow for datasets which will not fit into memory
- For all queries against a virtual product the database should be accessed to retrieve the locations of datasets, and then database should not be accessed again. The locations of the datasets may need to be serialized to a storage device and loaded later.
- Support querying multiple databases.

Class Thoughts
~~~~~~~~~~~~~~
A class implementing Virtual Products (VPs) will represent a tree which represents the hierarchy of virtual products. Leaf nodes will be Virtual Products which are unmodified products.

Combinators
    Combinators are functions which accept 1 or more Virtual Products and return a Virtual Product. The following combinators will be available:

    `Collate`: `List A -> A`
        For example: Combining the sensor readings for LS5 on 05/07/95, LS6 on 06/11/95, LS8 on 07/11/95 into one dataset.

    `Juxtapose`: `A -> B -> A x B`
        Similar to a `JOIN` in SQL, could be outer or inner. This could be used for apply PQ for cloud masking in combination with Filter or Transform.

    `Drop`: `A -> A`
        Given some predicate function, this will remove all datasets with metadata that do not meet the given predicate. Performed at query time.

    `Filter`: `A -> A`
        Given some predicate function, this will remove all points in a dataset that do not meet the given predicate. Performed at fetch time.

    `Transform`: `A -> B`
        Synonymous with the `map` function of the `mapreduce` paradigm. Will require some transformation function f which accepts a dataset and returns a dataset. This combinator may also modify the type of the dataset.

Methods & Workflow (High Level API)

    `validate`
        The validation function will check the measurement types of the inputs and outputs and ensure that they are compatible. In most cases this will be checking that they match, however with a `Transform` there may be a change.

    `query`
        The query function will retreieve datasets which match a query, product, and in case of `Drop` products, predicate. The product matched may be virtual. Access to the database will cease after this stage.

    `serialize`
        Optional(?) The serialize function will use the results of the `query` function (i.e. datasets that match the query) and serialize them for use on machines or nodes that cannot or should not query the database but can fetch data.

    `fetch`
        The fetch function will use the results of the `query`, direct or deserialized, and load the datasets. At this stage combinators working on data can execute; for example `Filter` can execute. Once completed, the output for the top level Virtual Product will be the desired Virtual Dataset.

Potential Approaches
--------------------
Virtual Product Module
~~~~~~~~~~~~~~~~~~~~~~
This approach would not modify any opendatacube-core code. Instead a new module would be created that defined `VirtualProduct`. This module would make use of the `datacube` module and API.

The module would query the properties of the `DatasetTypes` (a.k.a. products) used in the definition of the `VirtualProduct` and conduct type and sanity checking on combinators. After passing the checks the storage location of datasets for the given query (e.g. geoboxed or time bounded), the database would be queried using `datacube`. Advanced versions could intelligently walk the product tree to determine how to construct a SQL query for the various products and / or cache query results.

The module would then load the datasets from their location (e.g. disk, aws s3, etc.). Once the datasets are loaded, combinators are applied to the datasets and the results until the final result is created and returned. Again intelligent caching of result could be applied in this stage.