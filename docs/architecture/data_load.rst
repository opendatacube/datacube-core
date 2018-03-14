API
===
Datacube Load
-------------
The basics of the datacube load function are:

* `Find Datasets`_ - Look up the index​
* `Group Datasets`_ - Sort and group by time​ (and/or other dimensions)
* `Load Data`_ - Read from storage, write to output array

Find Datasets
~~~~~~~~~~~~~
Base on the user query, look up the index to find a list of `dataset` objects.

Group Datasets
~~~~~~~~~~~~~~
From the query we end up with a list of datacube Dataset objects.

We sort these over the non-spatial dimensions.​

(Currently the time dimension is hard-coded in a few places,
but the design allows for extension to arbitrary dimensions.)​

This can be the time stamp of the dataset, or a `solar_day` aggregation that group together based on time and
longitude, to avoid splitting days at UTC midnight.​

Load Data
~~~~~~~~~
For each group we created in the last step:​
    For each measurement (band):​
        For each dataset in the final chunk​
            Open the file for the dataset​
            Read the data for the requested area​
            Copy it to the output array in memory​

Load Data Lazily (optional)
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Instead of actually returning the data, we can just return the set of functions
that would return each portion of thedata as a computation graph.​

We can then chain more operations on computation graph, and then execute across
multiple processes on the computer, or across many computers.



GridWorkflow
------------
The GridWorkflow class allows for dividing a query along a spatial grid, allowing the data to be split.

Typically an application will match the output grid to the input grid,