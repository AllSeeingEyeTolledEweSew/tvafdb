#!/usr/bin/env python

from __future__ import with_statement

from setuptools import setup, find_packages

with open("README") as readme:
    documentation = readme.read()

setup(
    name="tvafdb",
    version="0.1.0",
    description="update-oriented document database, designed for tvaf",
    long_description=documentation,
    author="AllSeeingEyeTolledEweSew",
    author_email="allseeingeyetolledewesew@protonmail.com",
    url="http://github.com/AllSeeingEyeTolledEweSew/tvafdb",
    license="Unlicense",
    packages=find_packages(),
    use_2to3=True,
    entry_points={
        "console_scripts": [
            "tvafdb_server = tvafdb.server:main",
        ]
    },
)
