import sys
import setuptools


# Specify the relative directory where the source code is being stored
# This is the root directory for the namespaces, packages, modules, etc.
source_code_dir = "src"


with open('README.md', "r") as fh:
    long_description = fh.read()


# Specify the names of the pip packages which are required for this package to work
install_requires = [
    "numpy",
    "pandas",
    "stomp.py"
]


if sys.version_info < (3, 0):
    install_requires.append("future")


package_name = "Relay"


setuptools.setup(
    name=package_name,
    version="1.0.0",
    author="tschneider",
    author_email="tschneider@live.com",
    description="A collection of libraries for relays and callbacks.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(source_code_dir),
    package_dir={
        "": source_code_dir
    },
    install_requires= install_requires,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ]
)