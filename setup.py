#!/usr/bin/env python
import os
from distutils.core import setup

readme = os.path.join(os.path.dirname(__file__), "README.rst")
with open(readme) as f:
    long_description = f.read()

setup(
    name = "RedMate",
    version = "0.1",
    description = "Tools for migration from relational DB to redis",
    long_description = long_description,
    author = "Roman Kashitsyn",
    author_email = "roman.kashitsyn@gmail.com",
    license = "MIT",
    url = "https://github.com/roman-kashitsyn/redmate",
    classifiers = [
        "Programming Language :: Python",
        "Development Status :: 2 - Pre-Alpha",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Operation System :: OS Independent",
        "License :: OSI Approved :: MIT License",
        "Topic :: Utilities"
        ],
    packages = ["redmate"]
    )
