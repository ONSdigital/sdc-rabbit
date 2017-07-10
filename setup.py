#!/usr/bin/env python
# encoding: UTF-8

import ast
import os.path

from setuptools import setup

try:
    # For setup.py install
    from sdc.rabbit import __version__ as version
except ImportError:
    # For pip installations
    version = str(
        ast.literal_eval(
            open(os.path.join(
                os.path.dirname(__file__),
                "sdc", "rabbit", "__init__.py"),
                'r').read().split("=")[-1].strip()
        )
    )

installRequirements = [
    i.strip() for i in open(
        os.path.join(os.path.dirname(__file__), "requirements.txt"), 'r'
    ).readlines()
]

setup(
    name="sdc-rabbit-python",
    version=version,
    description="A shared library for SDC services that interact with RabbitMQ using Pika",
    author="J Gardiner",
    author_email="james@jgardiner.co.uk",
    url="https://github.com/ONSdigital/sdxc-rabbit-python",
    long_description=__doc__,
    classifiers=[
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
    ],
    packages=[
        "sdc.rabbit",
        "sdc.rabbit.test",
    ],
    package_data={
        "sdc.rabbit": [
            "requirements.txt",
        ],
        "sdc.rabbit.test": [
            "*.xml",
        ],
    },
    install_requires=installRequirements,
    extras_require={
        "dev": [
            "responses==0.5.1",
            "pep8>=1.6.2",
            "flake8>=2.6.0",
            "structlog==17.2.0",
        ],
    },
    tests_require=[
    ],
    entry_points={
        "console_scripts": [
        ],
    },
    zip_safe=False
)
