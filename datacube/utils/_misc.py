"""
Utility functions
"""
import os


class DatacubeException(Exception):
    """Your Data Cube has malfunctioned"""
    pass


def gen_password(num_random_bytes=12):
    """
    Generate random password
    """
    import base64
    return base64.urlsafe_b64encode(os.urandom(num_random_bytes)).decode('utf-8')
