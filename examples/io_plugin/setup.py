from setuptools import setup, find_packages

setup(
    name='dcio_example',
    version="1.0",
    description="Test IO plugins for datacube",
    author='AGDC Collaboration',
    packages=find_packages(),

    entry_points={
        'datacube.plugins.io.read': [
            'pickle=dcio_example.pickles:rdr_driver_init',
            'zeros=dcio_example.zeros:init_driver'
        ],
        'datacube.plugins.io.write': [
            'pickle=dcio_example.pickles:writer_driver_init',
        ]

    }
)
