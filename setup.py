from setuptools import setup,find_packages

setup(
    name='rambler.storage',
    version = '2.0',
    description='Rambler extension for object relationship mapping',
    author='Scott Robertson',
    author_email='srobertson@codeit.com',
    #package_dir = {'': 'src'},
    packages = find_packages(),
    install_requires = ['Rambler','searchable']

)
