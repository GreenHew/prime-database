from setuptools import setup, find_packages

setup(
    name = 'primedb',
    version = '1.0',
    url = 'https://github.com/GreenHew/prime-database',
    author = 'Matthew Green',
    packages = find_packages(exclude=['tests', 'docs'])
)