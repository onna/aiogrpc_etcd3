import aiogrpc_etcd3
from aiogrpc_etcd3.tests.containers import etcd_image

import pytest


@pytest.fixture(scope='session')
def etcd_server(request):
    host, port = etcd_image.run()

    def etcd_finalized():
            etcd_image.stop()

    request.addfinalizer(etcd_finalized)

    return host, port


@pytest.fixture(scope='function')
async def etcd(request, etcd_server):
    objclient = await aiogrpc_etcd3.client(
        host=etcd_server[0],
        port=etcd_server[1])
    yield objclient
    await objclient.delete_prefix('/')
    await objclient.finalize()
