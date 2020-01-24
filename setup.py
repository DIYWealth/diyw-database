from setuptools import setup, find_packages

required = [ 'requests',
             'pandas',
             'arrow',
             'socketIO-client-nexus',
             'pymongo',
             'numpy',
             'matplotlib',
             'peewee',
             'flask' ]

setup(
    name='iexscripts',
    version='0.1.0',
    description='Back-end processing for DIYWealth',
    author='John Walker and Robert Stainforth',
    url='https://github.com/DIYWealth',
    packages=['iexscripts',],
    install_requires=required
)
