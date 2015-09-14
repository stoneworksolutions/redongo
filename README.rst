*******
redongo
*******

.. image:: https://travis-ci.org/stoneworksolutions/redongo.svg?branch=0.1.7
    :target: https://travis-ci.org/stoneworksolutions/redongo

========
Synopsis
========

redongo is a utility written in Python that allows us to enqueue bulk saves to mongo through redis, it offers serialization for django objects and multiple Mongo connection and collections simultaneous usage. It's also horizontally scalable.

============
Code Example
============

Server:

.. code:: bash

    $ python server.py --redis dev-redis --redisdb 2 --redisqueue TEST

Client

.. code:: python

    import redongo_client
    redongo_client.set_application_settings(application_name, mongo_host, mongo_port, mongo_collection, mongo_user, mongo_password)
    redongo_client.save_to_mongo(application_name, objects_to_save)

==========
Motivation
==========

Needed a solution that allows to enqueue bulk saves to mongo

============
Installation
============


Using PIP

.. code:: bash

    $ pip install redongo

Downloading tar

.. code:: bash

    $ python setup.py install

=============
API Reference
=============

WIP

=====
Tests
=====

.. code:: bash

    $ py.test -v

============
Contributors
============

Stonework Solutions

=======
License
=======

Copyright (c) 2015 StoneWork Solutions

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
