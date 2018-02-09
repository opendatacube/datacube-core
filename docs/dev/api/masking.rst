.. _bit-masking:

Bit Masking
===========

Masking No Data Values
----------------------

.. currentmodule:: datacube.storage

.. automethod:: masking.mask_invalid_data

Masking with Bit-Flag Measurements
----------------------------------

One of the common types of data used with the Data Cube contains discrete
values stored within a numeric value. These values are often classifications
and outputs from tests, and need to be interpreted in specific ways, not as
simple scalar values. They are often used as a mask to exclude observations
which deemed unsuitable for a given analysis. For example, we want to exclude
observations of clouds when we are interested in what is on the ground.

Several methods are used when encoding these types of variables:

 - On-off bit flags, for a particular binary bit
 - Collections of bits that can indicate more than two possible values
 - Looking for a specific value stored using all available bits in the variable

From prior work, it is very easy to make mistakes when using these types of
variables, which can lead to processing the wrong set of observations, and
also making it quite difficult to read the code using them, and ensuring that
they are used in a consistent way in different places.

Open Data Cube provides a way of describing the meanings that can be encoded
in variables, which can then be used to give a readable method when using that
variable.

How to Define Meanings on Measurements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. warning::

   TODO

How to Create Masks within code
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. currentmodule:: datacube.storage

.. automethod:: masking.describe_variable_flags
.. automethod:: masking.make_mask
