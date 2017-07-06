# sdc-rabbit-python



[![Build Status](https://travis-ci.org/ONSdigital/sdc-rabbit-python.svg?branch=master)](https://www.codacy.com/app/ons-sdc/sdc-rabbit-python?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=ONSdigital/sdx-rabbit-python&amp;utm_campaign=Badge_Grade) [![codecov](https://codecov.io/gh/ONSdigital/sdc-rabbit-python/branch/master/graph/badge.svg)](https://codecov.io/gh/ONSdigital/sdc-rabbit-python) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/043810e79dac47759cc661361a8af12b)](https://www.codacy.com/app/ONS/sdc-rabbit-python?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=ONSdigital/sdc-rabbit-python&amp;utm_campaign=Badge_Grade)

Common code for SDC Pika-based services that interact with RabbitMQ.

## sdc-rabbit-python

A common source code library for SDC apps that use Pika to interact with RabbitMQ.
Apps wishing to use this service should use pip's VCS aware install method::

```Shell
    $ pip install git+git://github.com/ONSDigital/sdc-rabbit-python.git@master
```

For production deployments a tag should be referenced, rather than master.

### Basic Use

Assuming you are running a virtual environment:

###### Install requirements:

    $ make install

###### Run the unit tests:

    $ make test

###### Create a package for deployment:

    $ make sdist

###### Build documentation pages:

    $ make docs
