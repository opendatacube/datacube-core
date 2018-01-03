from setuptools import setup, find_packages

setup(
    name='dc_tests_io',
    version="1.0",
    description='Example "bad drivers" for testing driver loading protections',
    author='AGDC Collaboration',
    packages=find_packages(),

    entry_points={
        'datacube.plugins.io.read': [
            'bad_end_point=dc_tests_io.nosuch.module:init',
            'failing_end_point_throw=dc_tests_io.dummy:fail_to_init',
            'failing_end_point_none=dc_tests_io.dummy:init_to_none',
        ]
    }
)
