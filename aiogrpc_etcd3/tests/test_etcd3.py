"""
Tests for `etcd3` module.

----------------------------------
"""


import asyncio
import base64

import json

import os
import subprocess
import threading
import time

import aiogrpc_etcd3
import aiogrpc_etcd3.exceptions
import aiogrpc_etcd3.proto as etcdrpc
import aiogrpc_etcd3.utils as utils

import concurrent

import grpc

from hypothesis.strategies import characters

import pytest


pytestmark = pytest.mark.asyncio


def etcdctl(etcd_object, *args):
    os.environ['ETCDCTL_API'] = '3'
    args = ['--endpoints', etcd_object._url] + list(args)
    args = ['etcdctl', '-w', 'json'] + list(args)
    print(" ".join(args))
    output = subprocess.check_output(args)
    return json.loads(output.decode('utf-8'))


# def etcdctl2(*args):
#     # endpoint = os.environ.get('PYTHON_ETCD_HTTP_URL')
#     # if endpoint:
#     #     args = ['--endpoints', endpoint] + list(args)
#     # args = ['echo', 'pwd', '|', 'etcdctl', '-w', 'json'] + list(args)
#     # print(" ".join(args))
#     output = subprocess.check_output("echo pwd | ./etcdctl user add root")
#     return json.loads(output.decode('utf-8'))


class TestEtcd3(object):

    class MockedException(grpc.RpcError):
        def __init__(self, code):
            self._code = code

        def code(self):
            return self._code

    async def test_get_unknown_key(self, etcd):
        value, meta = await etcd.get('probably-invalid-key')
        assert value is None
        assert meta is None

    async def test_get_key(self, etcd):
        string = characters(blacklist_categories=['Cs', 'Cc']).example()
        etcdctl(etcd, 'put', '/doot/a_key', string)
        returned, _ = await etcd.get('/doot/a_key')
        assert returned == string.encode('utf-8')

    async def test_get_random_key(self, etcd):
        string = characters(blacklist_categories=['Cs', 'Cc']).example()
        etcdctl(etcd, 'put', '/doot/' + string, 'dootdoot')
        returned, _ = await etcd.get('/doot/' + string)
        assert returned == b'dootdoot'

    async def test_put_key(self, etcd):
        string = characters(blacklist_categories=['Cs', 'Cc']).example()
        await etcd.put('/doot/put_1', string)
        out = etcdctl(etcd, 'get', '/doot/put_1')
        assert base64.b64decode(out['kvs'][0]['value']) == \
            string.encode('utf-8')

    async def test_delete_key(self, etcd):
        etcdctl(etcd, 'put', '/doot/delete_this', 'delete pls')

        v, _ = await etcd.get('/doot/delete_this')
        assert v == b'delete pls'

        deleted = await etcd.delete('/doot/delete_this')
        assert deleted is True

        deleted = await etcd.delete('/doot/delete_this')
        assert deleted is False

        deleted = await etcd.delete('/doot/not_here_dude')
        assert deleted is False

        v, _ = await etcd.get('/doot/delete_this')
        assert v is None

    async def test_delete_keys_with_prefix(self, etcd):
        etcdctl(etcd, 'put', '/foo/1', 'bar')
        etcdctl(etcd, 'put', '/foo/2', 'baz')

        v, _ = await etcd.get('/foo/1')
        assert v == b'bar'

        v, _ = await etcd.get('/foo/2')
        assert v == b'baz'

        response = await etcd.delete_prefix('/foo')
        assert response.deleted == 2

        v, _ = await etcd.get('/foo/1')
        assert v is None

        v, _ = await etcd.get('/foo/2')
        assert v is None

    async def test_watch_key(self, etcd):
        def update_etcd(v):
            etcdctl(etcd, 'put', '/doot/watch', v)
            out = etcdctl(etcd, 'get', '/doot/watch')
            assert base64.b64decode(out['kvs'][0]['value']) == \
                utils.to_bytes(v)

        def update_key():
            # sleep to make watch can get the event
            time.sleep(3)
            update_etcd('0')
            time.sleep(1)
            update_etcd('1')
            time.sleep(1)
            update_etcd('2')
            time.sleep(1)
            update_etcd('3')
            time.sleep(1)

        t = threading.Thread(name="update_key", target=update_key)
        t.start()

        change_count = 0
        events_iterator, cancel = await etcd.watch(b'/doot/watch')
        async for event in events_iterator:
            assert event.key == b'/doot/watch'
            assert event.value == \
                utils.to_bytes(str(change_count))

            # if cancel worked, we should not receive event 3
            assert event.value != utils.to_bytes('3')

            change_count += 1
            if change_count > 2:
                # if cancel not work, we will block in this for-loop forever
                await cancel()

        t.join()

    async def test_watch_key_with_revision_compacted(self, etcd):
        etcdctl(etcd, 'put', '/random', '1')  # Some data to compact

        def update_etcd(v):
            etcdctl(etcd, 'put', '/watchcompation', v)
            out = etcdctl(etcd, 'get', '/watchcompation')
            assert base64.b64decode(out['kvs'][0]['value']) == \
                utils.to_bytes(v)

        def update_key():
            # sleep to make watch can get the event
            time.sleep(3)
            update_etcd('0')
            time.sleep(1)
            update_etcd('1')
            time.sleep(1)
            update_etcd('2')
            time.sleep(1)
            update_etcd('3')
            time.sleep(1)

        t = threading.Thread(name="update_key", target=update_key)
        t.start()

        async def watch_compacted_revision_test():
            events_iterator, cancel = await etcd.watch(
                b'/watchcompation', start_revision=1)

            error_raised = False
            compacted_revision = 0
            try:
                await events_iterator.__anext__()
            except Exception as err:
                error_raised = True
                assert isinstance(
                    err, aiogrpc_etcd3.exceptions.RevisionCompactedError)
                compacted_revision = err.compacted_revision

            assert error_raised is True
            assert compacted_revision == 2

            change_count = 0
            events_iterator, cancel = await etcd.watch(
                b'/watchcompation', start_revision=compacted_revision)
            async for event in events_iterator:
                assert event.key == b'/watchcompation'
                assert event.value == \
                    utils.to_bytes(str(change_count))

                # if cancel worked, we should not receive event 3
                assert event.value != utils.to_bytes('3')

                change_count += 1
                if change_count > 2:
                    await cancel()

        # Compact etcd and test watcher
        await etcd.compact(2)

        await watch_compacted_revision_test()

        t.join()

    # async def test_watch_exception_during_watch(self, etcd):
    #     def pass_exception_to_callback(callback):
    #         time.sleep(1)
    #         callback(self.MockedException(grpc.StatusCode.UNAVAILABLE))

    #     async def add_callback_mock(*args, **kwargs):
    #         callback = args[1]
    #         t = threading.Thread(name="pass_exception_to_callback",
    #                              target=pass_exception_to_callback,
    #                              args=[callback])
    #         t.start()
    #         return 1

    #     watcher_mock = mock.MagicMock()
    #     watcher_mock.add_callback = add_callback_mock
    #     etcd.watcher = watcher_mock

    #     events_iterator, cancel = await etcd.watch('foo')

    #     with pytest.raises(aiogrpc_etcd3.exceptions.ConnectionFailedError):
    #         async for _ in events_iterator:
    #             pass

    async def test_watch_timeout_on_establishment(self, etcd):
        foo_etcd = await aiogrpc_etcd3.client('foo.bar', timeout=3)

        with pytest.raises(concurrent.futures._base.TimeoutError):
            await foo_etcd.watch('foo')

    async def test_watch_prefix(self, etcd):
        def update_etcd(v):
            etcdctl(etcd, 'put', '/doot/watch/prefix/' + v, v)
            out = etcdctl(etcd, 'get', '/doot/watch/prefix/' + v)
            assert base64.b64decode(out['kvs'][0]['value']) == \
                utils.to_bytes(v)

        def update_key():
            # sleep to make watch can get the event
            time.sleep(3)
            update_etcd('0')
            time.sleep(1)
            update_etcd('1')
            time.sleep(1)
            update_etcd('2')
            time.sleep(1)
            update_etcd('3')
            time.sleep(1)

        t = threading.Thread(name="update_key_prefix", target=update_key)
        t.start()

        change_count = 0
        events_iterator, cancel = await etcd.watch_prefix(
            '/doot/watch/prefix/')
        async for event in events_iterator:
            assert event.key == \
                utils.to_bytes('/doot/watch/prefix/{}'.format(change_count))
            assert event.value == \
                utils.to_bytes(str(change_count))

            # if cancel worked, we should not receive event 3
            assert event.value != utils.to_bytes('3')

            change_count += 1
            if change_count > 2:
                # if cancel not work, we will block in this for-loop forever
                await cancel()

        t.join()

    async def test_sequential_watch_prefix_once(self, etcd):
        try:
            await etcd.watch_prefix_once('/doot/', 1)
        except concurrent.futures._base.TimeoutError:
            pass
        try:
            await etcd.watch_prefix_once('/doot/', 1)
        except concurrent.futures._base.TimeoutError:
            pass
        try:
            await etcd.watch_prefix_once('/doot/', 1)
        except concurrent.futures._base.TimeoutError:
            pass

    async def test_transaction_success(self, etcd):
        etcdctl(etcd, 'put', '/doot/txn', 'dootdoot')
        await etcd.transaction(
            compare=[etcd.transactions.value('/doot/txn') == 'dootdoot'],
            success=[etcd.transactions.put('/doot/txn', 'success')],
            failure=[etcd.transactions.put('/doot/txn', 'failure')]
        )
        out = etcdctl(etcd, 'get', '/doot/txn')
        assert base64.b64decode(out['kvs'][0]['value']) == b'success'

    async def test_transaction_failure(self, etcd):
        etcdctl(etcd, 'put', '/doot/txn', 'notdootdoot')
        await etcd.transaction(
            compare=[etcd.transactions.value('/doot/txn') == 'dootdoot'],
            success=[etcd.transactions.put('/doot/txn', 'success')],
            failure=[etcd.transactions.put('/doot/txn', 'failure')]
        )
        out = etcdctl(etcd, 'get', '/doot/txn')
        assert base64.b64decode(out['kvs'][0]['value']) == b'failure'

    async def test_ops_to_requests(self, etcd):
        with pytest.raises(Exception):
            etcd._ops_to_requests(['not_transaction_type'])
        with pytest.raises(TypeError):
            etcd._ops_to_requests(0)

    async def test_replace_success(self, etcd):
        await etcd.put('/doot/thing', 'toot')
        status = await etcd.replace('/doot/thing', 'toot', 'doot')
        v, _ = await etcd.get('/doot/thing')
        assert v == b'doot'
        assert status is True

    async def test_replace_fail(self, etcd):
        await etcd.put('/doot/thing', 'boot')
        status = await etcd.replace('/doot/thing', 'toot', 'doot')
        v, _ = await etcd.get('/doot/thing')
        assert v == b'boot'
        assert status is False

    async def test_get_prefix(self, etcd):
        for i in range(20):
            etcdctl(etcd, 'put', '/doot/range{}'.format(i), 'i am a range')

        for i in range(5):
            etcdctl(
                etcd, 'put', '/doot/notrange{}'.format(i), 'i am a not range')

        values = []
        async for x in etcd.get_prefix('/doot/range'):
            values.append(x)
        assert len(values) == 20
        for value, _ in values:
            assert value == b'i am a range'

    async def test_all_not_found_error(self, etcd):
        result = []
        async for x in etcd.get_all():
            result.append(x)
        assert not result

    async def test_range_not_found_error(self, etcd):
        for i in range(5):
            etcdctl(
                etcd, 'put', '/doot/notrange{}'.format(i), 'i am a not range')

        result = []
        async for x in etcd.get_prefix('/doot/range'):
            result.append(x)
        assert not result

    async def test_get_all(self, etcd):
        for i in range(20):
            etcdctl(etcd, 'put', '/doot/range{}'.format(i), 'i am in all')

        for i in range(5):
            etcdctl(etcd, 'put', '/doot/notrange{}'.format(i), 'i am in all')
        values = []
        async for x in etcd.get_all():
            values.append(x)
        assert len(values) == 25
        for value, _ in values:
            assert value == b'i am in all'

    async def test_sort_order(self, etcd):
        def remove_prefix(string, prefix):
            return string[len(prefix):]

        initial_keys = 'abcde'
        initial_values = 'qwert'

        for k, v in zip(initial_keys, initial_values):
            etcdctl(etcd, 'put', '/doot/{}'.format(k), v)

        keys = ''
        async for value, meta in etcd.get_prefix('/doot', sort_order='ascend'):
            keys += remove_prefix(meta.key.decode('utf-8'), '/doot/')

        assert keys == initial_keys

        reverse_keys = ''
        async for value, meta in etcd.get_prefix(
                '/doot', sort_order='descend'):
            reverse_keys += remove_prefix(meta.key.decode('utf-8'), '/doot/')

        assert reverse_keys == ''.join(reversed(initial_keys))

    async def test_lease_grant(self, etcd):
        lease = await etcd.lease(1)

        assert isinstance(lease.ttl, int)
        assert isinstance(lease.id, int)

    async def test_lease_revoke(self, etcd):
        lease = await etcd.lease(1)
        await lease.revoke()

    async def test_lease_keys_empty(self, etcd):
        lease = await etcd.lease(1)
        assert await lease.keys == []

    async def test_lease_single_key(self, etcd):
        lease = await etcd.lease(1)
        await etcd.put('/doot/lease_test', 'this is a lease', lease=lease)
        assert await lease.keys == [b'/doot/lease_test']

    async def test_lease_expire(self, etcd):
        key = '/doot/lease_test_expire'
        lease = await etcd.lease(1)
        await etcd.put(key, 'this is a lease', lease=lease)
        assert await lease.keys == [utils.to_bytes(key)]
        v, _ = await etcd.get(key)
        assert v == b'this is a lease'
        assert await lease.remaining_ttl <= await lease.granted_ttl

        # wait for the lease to expire
        gttl = await lease.granted_ttl
        await asyncio.sleep(gttl + 2)
        v, _ = await etcd.get(key)
        assert v is None

    async def test_member_list_single(self, etcd):
        # if tests are run against an etcd cluster rather than a single node,
        # this test will need to be changed
        members = []
        async for x in etcd.members:
            members.append(x)
        assert len(members) == 1
        for member in members:
            assert member.name.startswith('test-etcd-')
            for peer_url in member.peer_urls:
                assert peer_url.startswith('http://')
            for client_url in member.client_urls:
                assert client_url.startswith('http://')
            assert isinstance(member.id, int) is True

    async def test_lock_acquire(self, etcd):
        lock = etcd.lock('lock-1', ttl=10)
        assert await lock.acquire() is True
        value = await etcd.get(lock.key)
        assert value[0] is not None
        assert await lock.acquire(timeout=0) is False
        assert await lock.acquire(timeout=1) is False

    async def test_lock_release(self, etcd):
        lock = etcd.lock('lock-2', ttl=10)
        assert await lock.acquire() is True
        value = await etcd.get(lock.key)
        assert value[0] is not None
        assert await lock.release() is True
        v, _ = await etcd.get(lock.key)
        assert v is None
        assert await lock.acquire() is True
        assert await lock.release() is True
        assert await lock.acquire(timeout=None) is True

    async def test_lock_expire(self, etcd):
        lock = etcd.lock('lock-3', ttl=2)
        assert await lock.acquire() is True
        value = await etcd.get(lock.key)
        assert value[0] is not None
        # wait for the lease to expire
        await asyncio.sleep(6)
        v, _ = await etcd.get(lock.key)
        assert v is None

    async def test_lock_refresh(self, etcd):
        lock = etcd.lock('lock-4', ttl=2)
        assert await lock.acquire() is True
        value = await etcd.get(lock.key)
        assert value[0] is not None
        # sleep for the same total time as test_lock_expire, but refresh each
        # second
        for _ in range(6):
            asyncio.sleep(1)
            await lock.refresh()

        value = await etcd.get(lock.key)
        assert value[0] is not None

    async def test_lock_is_acquired(self, etcd):
        lock1 = etcd.lock('lock-5', ttl=2)
        assert await lock1.is_acquired() is False

        lock2 = etcd.lock('lock-5', ttl=2)
        await lock2.acquire()
        assert await lock2.is_acquired() is True
        await lock2.release()

        lock3 = etcd.lock('lock-5', ttl=2)
        await lock3.acquire()
        assert await lock3.is_acquired() is True
        assert await lock2.is_acquired() is False

    async def test_lock_context_manager(self, etcd):
        async with etcd.lock('lock-6', ttl=2) as lock:
            assert await lock.is_acquired() is True
        # lock2 = etcd.lock('lock-6', ttl=2)
        assert await lock.is_acquired() is False

    # async def test_lock_contended(self, etcd):
    #     lock1 = etcd.lock('lock-7', ttl=10)
    #     await lock1.acquire()
    #     lock2 = etcd.lock('lock-7', ttl=10)
    #     await lock2.acquire()
    #     assert await lock1.is_acquired() is False
    #     assert await lock2.is_acquired() is True

    async def test_lock_double_acquire_release(self, etcd):
        lock = etcd.lock('lock-8', ttl=10)
        assert await lock.acquire(0) is True
        assert await lock.acquire(0) is False
        assert await lock.release() is True

    async def test_lock_acquire_none(self, etcd):
        lock = etcd.lock('lock-9', ttl=10)
        assert await lock.acquire(None) is True
        # This will succeed after 10 seconds since the TTL will expire and the
        # lock is not refreshed
        assert await lock.acquire(None) is True

#     def test_internal_exception_on_internal_error(self, etcd):
#         exception = self.MockedException(grpc.StatusCode.INTERNAL)
#         kv_mock = mock.MagicMock()
#         kv_mock.Range.side_effect = exception
#         etcd.kvstub = kv_mock

#         with pytest.raises(aiogrpc_etcd3.exceptions.InternalServerError):
#             etcd.get("foo")

#     def test_connection_failure_exception_on_connection_failure(self, etcd):
#         exception = self.MockedException(grpc.StatusCode.UNAVAILABLE)
#         kv_mock = mock.MagicMock()
#         kv_mock.Range.side_effect = exception
#         etcd.kvstub = kv_mock

#         with pytest.raises(aiogrpc_etcd3.exceptions.ConnectionFailedError):
#             etcd.get("foo")

#     def test_connection_timeout_exception_on_connection_timeout(self, etcd):
#         exception = self.MockedException(grpc.StatusCode.DEADLINE_EXCEEDED)
#         kv_mock = mock.MagicMock()
#         kv_mock.Range.side_effect = exception
#         etcd.kvstub = kv_mock

#         with pytest.raises(aiogrpc_etcd3.exceptions.ConnectionTimeoutError):
#             etcd.get("foo")

#     def test_grpc_exception_on_unknown_code(self, etcd):
#         exception = self.MockedException(grpc.StatusCode.DATA_LOSS)
#         kv_mock = mock.MagicMock()
#         kv_mock.Range.side_effect = exception
#         etcd.kvstub = kv_mock

#         with pytest.raises(grpc.RpcError):
#             etcd.get("foo")

#     def test_status_member(self, etcd):
#         status = etcd.status()

#         assert isinstance(status.leader, aiogrpc_etcd3.members.Member) is \
#             True
#         assert status.leader.id in [m.id for m in etcd.members]

#     def test_hash(self, etcd):
#         assert isinstance(etcd.hash(), int)

#     def test_snapshot(self, etcd):
#         with tempfile.NamedTemporaryFile() as f:
#             etcd.snapshot(f)
#             f.flush()

#             etcdctl('snapshot', 'status', f.name)


# class TestAlarms(object):
#     @pytest.fixture
#     async def etcd(self):
#         etcd = await aiogrpc_etcd3.client()
#         yield etcd
#         etcd.disarm_alarm()
#         for m in etcd.members:
#             if m.active_alarms:
#                 etcd.disarm_alarm(m.id)

#     def test_create_alarm_all_members(self, etcd):
#         alarms = etcd.create_alarm()

#         assert len(alarms) == 1
#         assert alarms[0].member_id == 0
#         assert alarms[0].alarm_type == etcdrpc.NOSPACE

#     def test_create_alarm_specific_member(self, etcd):
#         a_member = next(etcd.members)

#         alarms = etcd.create_alarm(member_id=a_member.id)

#         assert len(alarms) == 1
#         assert alarms[0].member_id == a_member.id
#         assert alarms[0].alarm_type == etcdrpc.NOSPACE

#     def test_list_alarms(self, etcd):
#         a_member = next(etcd.members)
#         etcd.create_alarm()
#         etcd.create_alarm(member_id=a_member.id)
#         possible_member_ids = [0, a_member.id]

#         alarms = list(etcd.list_alarms())

#         assert len(alarms) == 2
#         for alarm in alarms:
#             possible_member_ids.remove(alarm.member_id)
#             assert alarm.alarm_type == etcdrpc.NOSPACE

#         assert possible_member_ids == []

#     def test_disarm_alarm(self, etcd):
#         etcd.create_alarm()
#         assert len(list(etcd.list_alarms())) == 1

#         etcd.disarm_alarm()
#         assert len(list(etcd.list_alarms())) == 0


class TestUtils(object):
    async def test_increment_last_byte(self):
        assert aiogrpc_etcd3.utils.increment_last_byte(b'foo') == b'fop'

    async def test_to_bytes(self):
        assert isinstance(aiogrpc_etcd3.utils.to_bytes(b'doot'), bytes) is True
        assert isinstance(aiogrpc_etcd3.utils.to_bytes('doot'), bytes) is True
        assert aiogrpc_etcd3.utils.to_bytes(b'doot') == b'doot'
        assert aiogrpc_etcd3.utils.to_bytes('doot') == b'doot'


# class TestEtcdTokenCallCredentials(object):

#     def test_token_callback(self):
#         e = EtcdTokenCallCredentials('foo')
#         callback = mock.MagicMock()
#         e(None, callback)
#         metadata = (('token', 'foo'),)
#         callback.assert_called_once_with(metadata, None)


# class TestClient(object):
#     @pytest.fixture
#     async def etcd(self):
#         yield await aiogrpc_etcd3.client()

#     def test_sort_target(self, etcd):
#         key = 'key'.encode('utf-8')
#         sort_target = {
#             None: etcdrpc.RangeRequest.KEY,
#             'key': etcdrpc.RangeRequest.KEY,
#             'version': etcdrpc.RangeRequest.VERSION,
#             'create': etcdrpc.RangeRequest.CREATE,
#             'mod': etcdrpc.RangeRequest.MOD,
#             'value': etcdrpc.RangeRequest.VALUE,
#         }

#         for input, expected in sort_target.items():
#             range_request = etcd._build_get_range_request(key,
#                                                           sort_target=input)
#             assert range_request.sort_target == expected
#         with pytest.raises(ValueError):
#             etcd._build_get_range_request(key, sort_target='feelsbadman')

#     def test_sort_order(self, etcd):
#         key = 'key'.encode('utf-8')
#         sort_target = {
#             None: etcdrpc.RangeRequest.NONE,
#             'ascend': etcdrpc.RangeRequest.ASCEND,
#             'descend': etcdrpc.RangeRequest.DESCEND,
#         }

#         for input, expected in sort_target.items():
#             range_request = etcd._build_get_range_request(key,
#                                                           sort_order=input)
#             assert range_request.sort_order == expected
#         with pytest.raises(ValueError):
#             etcd._build_get_range_request(key, sort_order='feelsbadman')

#     async def test_secure_channel(self):
#         client = await aiogrpc_etcd3.client(
#             ca_cert="tests/ca.crt",
#             cert_key="tests/client.key",
#             cert_cert="tests/client.crt"
#         )
#         assert client.uses_secure_channel is True

#     async def test_secure_channel_ca_cert_only(self):
#         client = await aiogrpc_etcd3.client(
#             ca_cert="tests/ca.crt",
#             cert_key=None,
#             cert_cert=None
#         )
#         assert client.uses_secure_channel is True

#     async def test_secure_channel_ca_cert_and_key_raise_exception(self):
#         with pytest.raises(ValueError):
#             await aiogrpc_etcd3.client(
#                 ca_cert='tests/ca.crt',
#                 cert_key='tests/client.crt',
#                 cert_cert=None)

#         with pytest.raises(ValueError):
#             await aiogrpc_etcd3.client(
#                 ca_cert='tests/ca.crt',
#                 cert_key=None,
#                 cert_cert='tests/client.crt')

#     def test_compact(self, etcd):
#         etcd.compact(3)
#         with pytest.raises(grpc.RpcError):
#             etcd.compact(3)

#     async def test_channel_with_no_cert(self):
#         client = await aiogrpc_etcd3.client(
#             ca_cert=None,
#             cert_key=None,
#             cert_cert=None
#         )
#         assert client.uses_secure_channel is False

#     @mock.patch('etcdrpc.AuthStub')
#     async def test_user_pwd_auth(self, auth_mock):
#         auth_resp_mock = mock.MagicMock()
#         auth_resp_mock.token = 'foo'
#         auth_mock.Authenticate = auth_resp_mock
#         self._enable_auth_in_etcd()

#         # Create a client using username and password auth
#         client = await aiogrpc_etcd3.client(
#             user='root',
#             password='pwd'
#         )

#         assert client.call_credentials is not None
#         self._disable_auth_in_etcd()

#     async def test_user_or_pwd_auth_raises_exception(self):
#         with pytest.raises(Exception):
#             await aiogrpc_etcd3.client(user='usr')

#         with pytest.raises(Exception):
#             await aiogrpc_etcd3.client(password='pwd')

#     def _enable_auth_in_etcd(self):
#         p = subprocess.Popen(
#             ['etcdctl', '-w', 'json', 'user', 'add', 'root'],
#             stdout=subprocess.PIPE,
#             stdin=subprocess.PIPE
#         )
#         password = 'pwd\n'
#         if six.PY3:
#             password = bytes(password, 'utf-8')
#         p.stdin.write(password)
#         p.stdin.write(password)
#         p.stdin.close()
#         subprocess.call(['etcdctl', 'auth', 'enable'])

#     def _disable_auth_in_etcd(self):
#         subprocess.call(['etcdctl', 'user', 'remove', 'root'])
#         subprocess.call(['etcdctl', '-u', 'root:pwd', 'auth', 'disable'])


class TestCompares(object):

    async def test_compare_version(self):
        key = 'key'
        tx = aiogrpc_etcd3.Transactions()

        version_compare = tx.version(key) == 1
        assert version_compare.op == etcdrpc.Compare.EQUAL

        version_compare = tx.version(key) != 2
        assert version_compare.op == etcdrpc.Compare.NOT_EQUAL

        version_compare = tx.version(key) < 91
        assert version_compare.op == etcdrpc.Compare.LESS

        version_compare = tx.version(key) > 92
        assert version_compare.op == etcdrpc.Compare.GREATER
        assert version_compare.build_message().target == \
            etcdrpc.Compare.VERSION

    async def test_compare_value(self):
        key = 'key'
        tx = aiogrpc_etcd3.Transactions()

        value_compare = tx.value(key) == 'b'
        assert value_compare.op == etcdrpc.Compare.EQUAL

        value_compare = tx.value(key) != 'b'
        assert value_compare.op == etcdrpc.Compare.NOT_EQUAL

        value_compare = tx.value(key) < 'b'
        assert value_compare.op == etcdrpc.Compare.LESS

        value_compare = tx.value(key) > 'b'
        assert value_compare.op == etcdrpc.Compare.GREATER
        assert value_compare.build_message().target == etcdrpc.Compare.VALUE

    async def test_compare_mod(self):
        key = 'key'
        tx = aiogrpc_etcd3.Transactions()

        mod_compare = tx.mod(key) == -100
        assert mod_compare.op == etcdrpc.Compare.EQUAL

        mod_compare = tx.mod(key) != -100
        assert mod_compare.op == etcdrpc.Compare.NOT_EQUAL

        mod_compare = tx.mod(key) < 19
        assert mod_compare.op == etcdrpc.Compare.LESS

        mod_compare = tx.mod(key) > 21
        assert mod_compare.op == etcdrpc.Compare.GREATER
        assert mod_compare.build_message().target == etcdrpc.Compare.MOD

    async def test_compare_create(self):
        key = 'key'
        tx = aiogrpc_etcd3.Transactions()

        create_compare = tx.create(key) == 10
        assert create_compare.op == etcdrpc.Compare.EQUAL

        create_compare = tx.create(key) != 10
        assert create_compare.op == etcdrpc.Compare.NOT_EQUAL

        create_compare = tx.create(key) < 155
        assert create_compare.op == etcdrpc.Compare.LESS

        create_compare = tx.create(key) > -12
        assert create_compare.op == etcdrpc.Compare.GREATER
        assert create_compare.build_message().target == etcdrpc.Compare.CREATE
