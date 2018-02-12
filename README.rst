========
aiogrpc_etcd3
========


.. image:: https://img.shields.io/pypi/v/aiogrpc_etcd3.svg
        :target: https://pypi.python.org/pypi/aiogrpc_etcd3

.. image:: https://img.shields.io/travis/onna/aiogrpc_etcd3.svg
        :target: https://travis-ci.org/onna/aiogrpc_etcd3

.. image:: https://readthedocs.org/projects/aiogrpc_etcd3/badge/?version=latest
        :target: https://aiogrpc_etcd3.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

.. image:: https://pyup.io/repos/github/onna/aiogrpc_etcd3/shield.svg
     :target: https://pyup.io/repos/github/onna/aiogrpc_etcd3/
     :alt: Updates

.. image:: https://codecov.io/github/onna/aiogrpc_etcd3/coverage.svg?branch=master
        :target: https://codecov.io/github/onna/aiogrpc_etcd3?branch=master


Python client for the etcd API v3, supported under python 3.6 and aioGRPC

**disclaimer**

There is a memory leak due to aiogrpc stream-stream
This code is a fork of https://github.com/kragniz/python-etcd3 with asyncio and aiogrpc

**Warning: the API is mostly stable, but may change in the future**

If you're interested in using this library, please get involved.

* Free software: Apache Software License 2.0
* Documentation: https://aiogrpc_etcd3.readthedocs.io.

Basic usage:

.. code-block:: python

    import aiogrpc_etcd3

    etcd = await aiogrpc_etcd3.client()

    await etcd.get('foo')
    await etcd.put('bar', 'doot')
    await etcd.delete('bar')

    # locks
    lock = etcd.lock('thing')
    # do something
    await lock.release()

    async with etcd.lock('doot-machine') as lock:
        # do something

    # transactions
    await etcd.transaction(
        compare=[
            etcd.transactions.value('/doot/testing') == 'doot',
            etcd.transactions.version('/doot/testing') > 0,
        ],
        success=[
            etcd.transactions.put('/doot/testing', 'success'),
        ],
        failure=[
            etcd.transactions.put('/doot/testing', 'failure'),
        ]
    )

    # watch key
    watch_count = 0
    events_iterator, cancel = etcd.watch("/doot/watch")
    async for event in events_iterator:
        print(event)
        watch_count += 1
        if watch_count > 10:
            await cancel()

    # watch prefix
    watch_count = 0
    events_iterator, cancel = etcd.watch_prefix("/doot/watch/prefix/")
    async for event in events_iterator:
        print(event)
        watch_count += 1
        if watch_count > 10:
            await cancel()

    # recieve watch events via callback function
    def watch_callback(event):
        print(event)

    watch_id = await etcd.add_watch_callback("/anotherkey", watch_callback)

    # cancel watch
    await etcd.cancel_watch(watch_id)
