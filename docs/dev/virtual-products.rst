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

Scope
~~~~~
Our principle design goals for Virtual Products (VPs) are:

- *A common interface for products*: Our representation of a product (both virtual and concrete)
  should provide methods to query data and load data (possibly among other things) so that the
  user need not be concerned with how a product is constructed (whether it is virtual or concrete,
  or performs on-the-fly computation).

- *Multi-product query optimization*: When observations from different products are expected
  to be in one-to-one correspondence (such as NBAR and PQ when producing cloud-free NBAR),
  the "missing" observations in the correspondence should be filtered out so that they are not
  actually loaded.

- *Multi-sensor aggregation*: A common pattern in scientific applications is to combine similar
  data (perhaps after some post-processing) collected by different sensors, such as Landsat 5, 7,
  or 8.

- *On-the-fly computation*: The ability to apply a data transformation to each observation as soon as
  the data is loaded. Often the actual loaded data is not needed afterwards and may be safely discarded.
  We aim to greatly reduce the peak resident memory size required for large scale computations.

Current design
~~~~~~~~~~~~~~
A Virtual Product in general is a tree. The leaf nodes are the concrete products in our datacube.
Other nodes in the tree represent modes of combining the data fetched from the leaf nodes
and transformations to be applied to that data.

Combinators
    Combinators are functions which accept 1 or more Virtual Products and return a Virtual Product. The following combinators will be available:

    `Collate`: `List A -> A`
        For example: Combining the sensor readings for LS5 on 05/07/95, LS6 on 06/11/95, LS8 on 07/11/95 into one dataset.

    `Juxtapose`: `A -> B -> A x B`
        Similar to a `JOIN` in SQL, could be outer or inner. This could be used for apply PQ for cloud masking in combination with Transform.

    `Transform`: `A -> B`
        Synonymous with the `map` function of the `mapreduce` paradigm. Will require some transformation function which accepts a dataset and returns a dataset. This combinator may also modify the type of the dataset.

Methods & Workflow (High Level API)

    `construct`
        Constructing a Virtual Product will set its child Virtual Products as well as any functions it requires to operate. The construction will not access the database.

    `validate`
        The validation function will check the measurement types of the inputs and outputs and ensure that they are compatible. In most cases this will be checking that they match, however with a `Transform` there may be a change.

    `query`
        The query function will retreieve datasets which match a query. The product being queried may be virtual. Access to the database will cease after this stage.

    `fetch`
        The fetch function will use the results of the `query`, direct or deserialized, and load the datasets. At this stage combinators working on data can apply their functions or predicates; for example `Filter` can execute. Once completed, the output for the top level Virtual Product will be the desired Virtual Dataset.

Potential Approaches
--------------------
Virtual Product Module
~~~~~~~~~~~~~~~~~~~~~~
This approach would not modify any opendatacube-core code. Instead a new module would be created that defined `VirtualProduct`. This module would make use of the `datacube` module and API.

The module would query the properties of the `DatasetTypes` (a.k.a. products) used in the definition of the `VirtualProduct` and conduct type and sanity checking on combinators. After passing the checks the storage location of datasets for the given query (e.g. geoboxed or time bounded), the database would be queried using `datacube`. Advanced versions could intelligently walk the product tree to determine how to construct a SQL query for the various products and / or cache query results.

The module would then load the datasets from their location (e.g. disk, aws s3, etc.). Once the datasets are loaded, combinators are applied to the datasets and the results until the final result is created and returned. Again intelligent caching of result could be applied in this stage.
