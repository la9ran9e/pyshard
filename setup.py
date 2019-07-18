from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='pyshard',
    version='0.0.1',
    description='Distributed key-value storage in Python stdlib',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='la9ran9e',
    author_email="tvauritimur@gmail.com",
    url='https://github.com/la9ran9e/pyshard',
    packages=[
        'pyshard',
        'pyshard.app',
        'pyshard.core',
        'pyshard.master',
        'pyshard.shard',
        'pyshard.settings',
        'pyshard.utils'
    ]
)
