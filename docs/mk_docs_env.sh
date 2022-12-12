#!/usr/bin/env bash

. "/home/omad/miniconda3/etc/profile.d/mamba.sh"

mamba create -p .condaenv python=3.10
conda activate ./.condaenv
mamba install --only-deps datacube
mamba install sphinx sphinx-click pydata-sphinx-theme sphinx-autodoc-typehints autodocsumm nbsphinx ipython sphinx-autobuild
pip install --no-deps -e ..
