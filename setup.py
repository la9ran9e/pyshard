from setuptools import setup

setup(
    name='pyshard',
    version='0.1',
    description='Distributed key-value storage in Python stdlib',
    author='la9ran9e',
    uri='https://github.com/la9ran9e/pyshard',
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
