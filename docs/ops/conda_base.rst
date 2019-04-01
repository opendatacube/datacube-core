3. Add the conda-forge channel
    .. code::

        conda config --add channels conda-forge

    The conda-forge channel provides multitude of community maintained packages.
    Find out more about it here https://conda-forge.org/

4. Create a virtual environment in conda
    .. code::

        conda create --name cubeenv python=3.6 datacube

5. Activate the virtual environment
    .. code::

        source activate cubeenv

    Find out more about managing virtual environments here https://conda.io/docs/using/envs.html

6. Install other packages
    .. code::

        conda install jupyter matplotlib scipy

    Find out more about managing packages here https://conda.io/docs/using/pkgs.html
