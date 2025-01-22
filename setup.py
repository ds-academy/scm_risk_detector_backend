# setup.py
from setuptools import setup, find_packages


with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='module',
    version='0.0.1',
    packages=find_packages(),
    description='SCM risk detector based on ML',
    install_requires=[
        requirements
    ],
)


