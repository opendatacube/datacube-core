from setuptools import setup, find_packages

setup(
    name='dcio_example',
    version="1.0",
    description="Test IO plugins for datacube",
    author='AGDC Collaboration',
    packages=find_packages(),

    entry_points={
        'datacube.plugins.io.read': [
            'pickle=dcio_example.pickles:init_driver',
            'zeros=dcio_example.zeros:init_driver'
        ]
    }
)
