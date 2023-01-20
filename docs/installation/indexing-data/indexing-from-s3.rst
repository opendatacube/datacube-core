Indexing data from Amazon (AWS S3)
==================================

Options currently exist that allow for a user to store, index, and retrieve data
from cloud object stores, such as Amazon S3 buckets, using the open ODC.
While the process is largely the same as the step by step guide there are a few additional requirements outline below.

Configuring AWS CLI Credentials
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Install the AWS CLI package and configure it with your Amazon AWS credentials.
For a more detailed tutorial on AWS CLI configurations, visit the
`official AWS docs <https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html>`_.
The only two fields required to be configured are the ``Access Key``, and
``Secret Access Key``. These keys can be found on your AWS login
security page. Try not to lose your ``Secret Access Key`` as you will
not be able to view it again and you will have to request a new one.


Install AWS S3 Indexing Scripts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In order to utilize the convenience of S3 indexing, we must install
some tools that will help make it easier and faster. You can find the code
and further detailed documentation for the tools used below in the
`odc-tools <https://github.com/opendatacube/odc-tools/tree/develop/apps/dc_tools>`_ repository.

.. code-block:: bash

    pip install odc_apps_dc_tools

S3 Indexing Example
~~~~~~~~~~~~~~~~~~~

For this example we will be indexing Digital Earth Australia's public data bucket,
which you can browse at `data.dea.ga.gov.au <https://data.dea.ga.gov.au>`_.

Run the two lines below, the first will add the product definition for the Landsat
Geomedian product and the second will add all of the Geomedian datasets. This will
take some time, but will add a continental product to your local Datacube.

.. code-block:: bash

    datacube product add https://data.dea.ga.gov.au/geomedian-australia/v2.1.0/product-definition.yaml
    s3-to-dc --no-sign-request 's3://dea-public-data/geomedian-australia/v2.1.0/L8/**/*.yaml' ls8_nbart_geomedian_annual

Congratulations, you've now indexed data from the Digital Earth Australia buckets 🎉
