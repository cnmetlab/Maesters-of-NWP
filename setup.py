import os
from setuptools import find_packages, setup

BASE_PATH = os.path.dirname(os.path.realpath(__file__))

with open(os.path.join(BASE_PATH, "README.md"), "r") as f:
    description = f.read()

with open(os.path.join(BASE_PATH, "requirements.txt")) as f:
    required = f.read().splitlines()

setup(
    name="maesters-nwp",
    version="0.0.9",
    author="blizhan",
    author_email="blizhan@icloud.com",
    description="A package to get open NWP data in a elegant way",
    long_description=description,
    long_description_content_type="text/markdown",
    url="https://github.com/cnmetlab/Maesters-of-NWP",
    package_dir={"maesters": "maesters", ".": "./"},
    package_data={
        "maesters": ["static/pf_split", "static/*/*.nc", "static/*/*.txt"],
        ".": ["*.txt"],
        "": ["*.toml", "*.txt"],
    },
    include_package_data=True,
    # package_data={"": ["*.toml","*.txt"]},
    packages=find_packages(),
    install_requires=required,
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    python_requires=">=3.7",
)
