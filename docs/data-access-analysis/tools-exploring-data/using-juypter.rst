Using Juypter Notebooks
=============================

One of the most common ways to use an Open Data Cube is through interactively writing Python code within a Jupyter Notebook. This allows dynamically loading data, performing analysis and developing scientific algorithms.


.. _Jupyter Notebooks: https://jupyter.org/

Several GitHub repositories of example Open Data Cube notebooks are available, showing
how to access data through ODC, along with example algorithms and visualisations:


Digital Earth Australia Notebooks
---------------------------------
.. image:: https://raw.githubusercontent.com/GeoscienceAustralia/dea-notebooks/develop/Supplementary_data/dea_logo_wide.jpg
  :width: 900
  :alt: Digital Earth Australia Notebooks banner
  :target: https://github.com/GeoscienceAustralia/dea-notebooks/

`Digital Earth Australia Notebooks`_ hosts Jupyter Notebooks, Python scripts and workflows for analysing data from the Digital Earth Australia (DEA) instance of the Open Data Cube. This documentation provides a guide to the wide range of geospatial analyses that can be achieved using Open Data Cube and ``xarray``. The repository contains the following key content:

* `Beginners guide`_: Introductory notebooks aimed at introducing Jupyter Notebooks and how to load, plot and interact with Open Data Cube data
* `Frequently used code`_: A recipe book of simple code examples demonstrating how to perform common geospatial analysis tasks using Open Data Cube
* `Real world examples`_: More complex workflows demonstrating how Open Data Cube can be used to address real-world challenges

.. _`Digital Earth Australia Notebooks`: https://github.com/GeoscienceAustralia/dea-notebooks/
.. _`Beginners guide`: https://docs.dea.ga.gov.au/notebooks/Beginners_guide/README.html
.. _`Frequently used code`: https://docs.dea.ga.gov.au/notebooks/Frequently_used_code/README.html
.. _`Real world examples`: https://docs.dea.ga.gov.au/notebooks/Real_world_examples/README.html


Digital Earth Africa Notebooks
------------------------------
.. image:: https://raw.githubusercontent.com/digitalearthafrica/deafrica-sandbox-notebooks/master/Supplementary_data/Github_banner.jpg
  :width: 900
  :alt: Digital Earth Africa Notebooks banner
  :target: https://github.com/digitalearthafrica/deafrica-sandbox-notebooks/

`Digital Earth Africa Notebooks`_ provides a similarly comprehensive repository of Jupyter notebooks and code that allow users to use, interact and engage with data from the Digital Earth Africa instance of the Open Data Cube. This includes code examples based on USGS Landsat Collection 2, Level 2 and Copernicus Sentinel-2 Level 2A data that are available globally for use in Open Data Cube implementations.

.. _`Digital Earth Africa Notebooks`: https://github.com/digitalearthafrica/deafrica-sandbox-notebooks/


DEA and DE Africa Tools code
----------------------------

Both `Digital Earth Australia Notebooks`_ and `Digital Earth Africa Notebooks`_ provide pip-installable Python modules containing useful tools for analysing Open Data Cube data, including functions for loading and plotting satellite imagery, calculating band indices, analysing spatial datasets, and machine learning. These tools can be accessed here:

* ``DEA Tools``: https://github.com/GeoscienceAustralia/dea-notebooks/tree/stable/Tools
* ``DE Africa Tools``: https://github.com/digitalearthafrica/deafrica-sandbox-notebooks/tree/master/Tools
