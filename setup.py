
import os
from setuptools import find_packages, setup

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'README.md'), 'r') as f:
    description = f.read()
requirements_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'requirements.txt')
with open(requirements_path) as f:
    required = f.read().splitlines()

setup(
    name='maesters-nwp',
    version='0.0.1',
    author='blizha',
    author_email='blizhan@icloud.com',
    description='A package to get Open NWP data in a elegant way',
    long_description=description,
    long_description_content_type='text/markdown',
    url='https://github.com/blizhan/Maesters-of-NWP',
    include_package_data=True,
    package_data={'': ['*.nc', '*.toml']},
    packages=find_packages(),
    install_requires=required,
    classifiers=[
        'Programming Language :: Python :: 3',
    ],
    python_requires='>=3.8'
)