

Connecting to a Datacube
=============================

The Datacube Class is how you establish a connection to an existing Datacube instance.

Minimal Example
~~~~~~~~~~~~~~~~
After importing the ``datacube`` package, users need to specify a name for their session, known as the ``app`` name. This name is chosen by the user and is used to track down issues with database queries. It does not have any effect on the analysis. The resulting ``dc`` object provides access to all the data contained within the datacube instance.

.. code-block:: python

    import datacube

    dc = datacube.Datacube(app="my_analysis")

.. admonition:: Note
  :class: attention

  If you have trouble connecting make sure your environment is setup correctly, see **Setting up your environment**

API
~~~~~~~~~~~~~~~~
See the API docs below for a completed description of the available options.

.. currentmodule:: datacube

.. autosummary::

   Datacube
