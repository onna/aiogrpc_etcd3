
import pytest
pytestmark = pytest.mark.asyncio


async def test_set_key(etcd):
    await etcd.put('foo', 'bar')
    result = await etcd.get('foo')
    assert result[0] == b'bar'


async def test_create_folder(etcd):
    await etcd.put('/food/1', '1')
    await etcd.put('/food/2', '2')
    await etcd.put('/food/3', '3')
    count = 0
    async for element in etcd.get_prefix('/food'):  # noqa
        count += 1

    assert count == 3


async def test_delete_api(etcd):
    await etcd.put('foo', 'bar')
    result = await etcd.get('foo')
    assert result[0] == b'bar'
    await etcd.delete('foo')
    result = await etcd.get('foo')
    assert (result[0] is None) and (result[1]is None)
