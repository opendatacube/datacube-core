from setuptools import setup, find_packages

setup(
    name='dcio',
    version="1.0",
    description="Test IO plugins for datacube",
    author='AGDC Collaboration',
    packages=find_packages(),

    entry_points={
        'datacube.plugins.io.read': [
            'pickle=dcio.pickles:init_driver',
            'zeros=dcio.zeros:init_driver'
        ]
    }
)
