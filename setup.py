import codecs
import os
import re
from setuptools import setup, find_packages
"""
git tag {VERSION}
git push --tags
python setup.py sdist
python setup.py bdist_wheel --universal
twine upload dist/*
"""
def read(*parts):
    with codecs.open(os.path.join(HERE, *parts), 'r') as fp:
        return fp.read()
def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    else:
        return "UNKNOWN"
HERE = os.path.abspath(os.path.dirname(__file__))
VERSION = find_version("colocarpy", "version.py")
setup(
    name="colocarpy",
    version=VERSION,
    author="Jordan Matelsky",
    author_email="jordan.matelsky@jhuapl.edu",
    description=("Python client for colocard"),
    license="Apache 2.0",
    keywords="",
    url="https://github.com/aplbrain/colocarpy/tarball/" + VERSION,
    packages=find_packages(),
    install_requires=[
        "matplotlib",
        "networkx",
        "pandas",
        "requests",
        "typing",
        "typing-extensions",
    ],
    extras_require={
        "dev": [
            "pylint",
            "mypy",
        ]
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
    ]
)

