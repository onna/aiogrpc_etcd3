import asyncio

import functools
import inspect

from aiogrpc_etcd3 import exceptions as exceptions
from aiogrpc_etcd3 import leases as leases
import aiogrpc_etcd3.locks as locks
import aiogrpc_etcd3.members
import aiogrpc_etcd3.transactions as transactions
import aiogrpc_etcd3.utils as utils
import aiogrpc_etcd3.watch as watch

import aiogrpc

import grpc
import grpc._channel

from .proto.rpc_pb2 import (
    AlarmRequest, AuthenticateRequest, CompactionRequest, DefragmentRequest,
    DeleteRangeRequest, HashRequest, LeaseGrantRequest, LeaseKeepAliveRequest,
    LeaseRevokeRequest, LeaseTimeToLiveRequest,
    MemberAddRequest, MemberListRequest, MemberRemoveRequest,
    MemberUpdateRequest, NONE, NOSPACE, PutRequest, RangeRequest,
    RequestOp, SnapshotRequest, StatusRequest, TxnRequest)
from .proto.rpc_pb2_grpc import (
    AuthStub, ClusterStub, KVStub, LeaseStub, MaintenanceStub, WatchStub)


_EXCEPTIONS_BY_CODE = {
    grpc.StatusCode.INTERNAL: exceptions.InternalServerError,
    grpc.StatusCode.UNAVAILABLE: exceptions.ConnectionFailedError,
    grpc.StatusCode.DEADLINE_EXCEEDED: exceptions.ConnectionTimeoutError,
    grpc.StatusCode.FAILED_PRECONDITION: exceptions.PreconditionFailedError,
}


def _translate_exception(exc):
    code = exc.code()
    exception = _EXCEPTIONS_BY_CODE.get(code)
    if exception is None:
        raise
    raise exception


def _handle_errors(f):
    if inspect.isgeneratorfunction(f):
        def handler(*args, **kwargs):
            try:
                for data in f(*args, **kwargs):
                    yield data
            except grpc.RpcError as exc:
                _translate_exception(exc)
    else:
        def handler(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except grpc.RpcError as exc:
                _translate_exception(exc)

    return functools.wraps(f)(handler)


class Transactions(object):
    def __init__(self):
        self.value = transactions.Value
        self.version = transactions.Version
        self.create = transactions.Create
        self.mod = transactions.Mod

        self.put = transactions.Put
        self.get = transactions.Get
        self.delete = transactions.Delete


class KVMetadata(object):
    def __init__(self, keyvalue):
        self.key = keyvalue.key
        self.create_revision = keyvalue.create_revision
        self.mod_revision = keyvalue.mod_revision
        self.version = keyvalue.version
        self.lease_id = keyvalue.lease


class Status(object):
    def __init__(self, version, db_size, leader, raft_index, raft_term):
        self.version = version
        self.db_size = db_size
        self.leader = leader
        self.raft_index = raft_index
        self.raft_term = raft_term


class Alarm(object):
    def __init__(self, alarm_type, member_id):
        self.alarm_type = alarm_type
        self.member_id = member_id


class EtcdTokenCallCredentials(grpc.AuthMetadataPlugin):
    """Metadata wrapper for raw access token credentials."""

    def __init__(self, access_token):
        self._access_token = access_token

    def __call__(self, context, callback):
        metadata = (('token', self._access_token),)
        callback(metadata, None)


class Etcd3Client(object):
    def __init__(self, host='localhost', port=2379,
                 ca_cert=None, cert_key=None, cert_cert=None, timeout=None,
                 user=None, password=None, loop=None):

        if loop is None:
            loop = asyncio.get_event_loop()

        self._url = '{host}:{port}'.format(host=host, port=port)

        cert_params = [c is not None for c in (cert_cert, cert_key)]
        if ca_cert is not None:
            if all(cert_params):
                credentials = self._get_secure_creds(
                    ca_cert,
                    cert_key,
                    cert_cert
                )
                self.uses_secure_channel = True
                self.channel = aiogrpc.secure_channel(
                    self._url, credentials, loop=loop)
            elif any(cert_params):
                # some of the cert parameters are set
                raise ValueError(
                    'to use a secure channel ca_cert is required by itself, '
                    'or cert_cert and cert_key must both be specified.')
            else:
                credentials = self._get_secure_creds(ca_cert, None, None)
                self.uses_secure_channel = True
                self.channel = aiogrpc.secure_channel(
                    self._url, credentials, loop=loop)
        else:
            self.uses_secure_channel = False
            self.channel = aiogrpc.insecure_channel(self._url, loop=loop)

        self.timeout = timeout
        self.call_credentials = None
        self._user = user
        self._password = password

        self._loop = loop

    async def initialization(self):

        cred_params = [c is not None for c in (self._user, self._password)]

        if all(cred_params):
            self.auth_stub = AuthStub(self.channel)
            auth_request = AuthenticateRequest(
                name=self._user,
                password=self._password
            )

            resp = await self.auth_stub.Authenticate(
                auth_request, self.timeout)
            self.call_credentials = grpc.metadata_call_credentials(
                EtcdTokenCallCredentials(resp.token))

        elif any(cred_params):
            raise Exception(
                'if using authentication credentials both user and password '
                'must be specified.'
            )

        self.kvstub = KVStub(self.channel)
        self.watcher = watch.Watcher(
            WatchStub(self.channel),
            timeout=self.timeout,
            call_credentials=self.call_credentials,
            loop=self._loop
        )
        self.watchtask = self._loop.create_task(self.watcher.run())
        self.clusterstub = ClusterStub(self.channel)
        self.leasestub = LeaseStub(self.channel)
        self.maintenancestub = MaintenanceStub(self.channel)
        self.transactions = Transactions()

    async def finalize(self):
        self.watchtask.cancel()
        if asyncio.iscoroutinefunction(self.watcher.stop):
            await self.watcher.stop()
        # Make sure we close
        await asyncio.sleep(1)

    def _get_secure_creds(self, ca_cert, cert_key=None, cert_cert=None):
        cert_key_file = None
        cert_cert_file = None

        with open(ca_cert, 'rb') as f:
            ca_cert_file = f.read()

        if cert_key is not None:
            with open(cert_key, 'rb') as f:
                cert_key_file = f.read()

        if cert_cert is not None:
            with open(cert_cert, 'rb') as f:
                cert_cert_file = f.read()

        return grpc.ssl_channel_credentials(
            ca_cert_file,
            cert_key_file,
            cert_cert_file
        )

    def _build_get_range_request(self, key,
                                 range_end=None,
                                 limit=None,
                                 revision=None,
                                 sort_order=None,
                                 sort_target='key',
                                 serializable=None,
                                 keys_only=None,
                                 count_only=None,
                                 min_mod_revision=None,
                                 max_mod_revision=None,
                                 min_create_revision=None,
                                 max_create_revision=None):
        range_request = RangeRequest()
        range_request.key = utils.to_bytes(key)
        if range_end is not None:
            range_request.range_end = utils.to_bytes(range_end)

        if sort_order is None:
            range_request.sort_order = RangeRequest.NONE
        elif sort_order == 'ascend':
            range_request.sort_order = RangeRequest.ASCEND
        elif sort_order == 'descend':
            range_request.sort_order = RangeRequest.DESCEND
        else:
            raise ValueError('unknown sort order: "{}"'.format(sort_order))

        if sort_target is None or sort_target == 'key':
            range_request.sort_target = RangeRequest.KEY
        elif sort_target == 'version':
            range_request.sort_target = RangeRequest.VERSION
        elif sort_target == 'create':
            range_request.sort_target = RangeRequest.CREATE
        elif sort_target == 'mod':
            range_request.sort_target = RangeRequest.MOD
        elif sort_target == 'value':
            range_request.sort_target = RangeRequest.VALUE
        else:
            raise ValueError('sort_target must be one of "key", '
                             '"version", "create", "mod" or "value"')

        return range_request

    @_handle_errors
    async def get(self, key):
        """
        Get the value of a key from etcd.

        example usage:

        .. code-block:: python

            >>> import etcd3
            >>> etcd = etcd3.client()
            >>> etcd.get('/thing/key')
            'hello world'

        :param key: key in etcd to get
        :returns: value of key and metadata
        :rtype: bytes, ``KVMetadata``
        """
        range_request = self._build_get_range_request(key)
        range_response = await self.kvstub.Range(
            range_request,
            self.timeout,
            credentials=self.call_credentials,
        )

        if range_response.count < 1:
            return None, None
        else:
            kv = range_response.kvs.pop()
            return kv.value, KVMetadata(kv)

    @_handle_errors
    async def get_prefix(self, key_prefix, sort_order=None, sort_target='key'):
        """
        Get a range of keys with a prefix.

        :param key_prefix: first key in range

        :returns: sequence of (value, metadata) tuples
        """
        range_request = self._build_get_range_request(
            key=key_prefix,
            range_end=utils.increment_last_byte(utils.to_bytes(key_prefix)),
            sort_order=sort_order,
        )

        range_response = await self.kvstub.Range(
            range_request,
            self.timeout,
            credentials=self.call_credentials,
        )

        if range_response.count < 1:
            return
        else:
            for kv in range_response.kvs:
                yield (kv.value, KVMetadata(kv))

    @_handle_errors
    async def get_all(self, sort_order=None, sort_target='key'):
        """
        Get all keys currently stored in etcd.

        :returns: sequence of (value, metadata) tuples
        """
        range_request = self._build_get_range_request(
            key=b'\0',
            range_end=b'\0',
            sort_order=sort_order,
            sort_target=sort_target,
        )

        range_response = await self.kvstub.Range(
            range_request,
            self.timeout,
            credentials=self.call_credentials,
        )

        if range_response.count < 1:
            return
        else:
            for kv in range_response.kvs:
                yield (kv.value, KVMetadata(kv))

    def _build_put_request(self, key, value, lease=None):
        put_request = PutRequest()
        put_request.key = utils.to_bytes(key)
        put_request.value = utils.to_bytes(value)
        put_request.lease = utils.lease_to_id(lease)
        return put_request

    @_handle_errors
    async def put(self, key, value, lease=None):
        """
        Save a value to etcd.

        Example usage:

        .. code-block:: python

            >>> import etcd3
            >>> etcd = etcd3.client()
            >>> etcd.put('/thing/key', 'hello world')

        :param key: key in etcd to set
        :param value: value to set key to
        :type value: bytes
        :param lease: Lease to associate with this key.
        :type lease: either :class:`.Lease`, or int (ID of lease)
        """
        put_request = self._build_put_request(key, value, lease=lease)
        await self.kvstub.Put(
            put_request,
            self.timeout,
            credentials=self.call_credentials,
        )

    @_handle_errors
    async def replace(self, key, initial_value, new_value):
        """
        Atomically replace the value of a key with a new value.

        This compares the current value of a key, then replaces it with a new
        value if it is equal to a specified value. This operation takes place
        in a transaction.

        :param key: key in etcd to replace
        :param initial_value: old value to replace
        :type initial_value: bytes
        :param new_value: new value of the key
        :type new_value: bytes
        :returns: status of transaction, ``True`` if the replace was
                  successful, ``False`` otherwise
        :rtype: bool
        """
        status, _ = await self.transaction(
            compare=[self.transactions.value(key) == initial_value],
            success=[self.transactions.put(key, new_value)],
            failure=[],
        )

        return status

    def _build_delete_request(self, key,
                              range_end=None,
                              prev_kv=None):
        delete_request = DeleteRangeRequest()
        delete_request.key = utils.to_bytes(key)

        if range_end is not None:
            delete_request.range_end = utils.to_bytes(range_end)

        if prev_kv is not None:
            delete_request.prev_kv = prev_kv

        return delete_request

    @_handle_errors
    async def delete(self, key):
        """
        Delete a single key in etcd.

        :param key: key in etcd to delete
        :returns: True if the key has been deleted
        """
        delete_request = self._build_delete_request(key)
        delete_response = await self.kvstub.DeleteRange(
            delete_request,
            self.timeout,
            credentials=self.call_credentials,
        )
        return delete_response.deleted >= 1

    @_handle_errors
    async def delete_prefix(self, prefix):
        """Delete a range of keys with a prefix in etcd."""
        delete_request = self._build_delete_request(
            prefix,
            range_end=utils.increment_last_byte(utils.to_bytes(prefix))
        )
        return await self.kvstub.DeleteRange(
            delete_request,
            self.timeout,
            credentials=self.call_credentials,
        )

    @_handle_errors
    async def status(self):
        """Get the status of the responding member."""
        status_request = StatusRequest()
        status_response = await self.maintenancestub.Status(
            status_request,
            self.timeout,
            credentials=self.call_credentials,
        )

        for m in self.members:
            if m.id == status_response.leader:
                leader = m
                break
        else:
            # raise exception?
            leader = None

        return Status(status_response.version,
                      status_response.dbSize,
                      leader,
                      status_response.raftIndex,
                      status_response.raftTerm)

    @_handle_errors
    async def add_watch_callback(self, *args, **kwargs):
        """
        Watch a key or range of keys and call a callback on every event.

        If timeout was declared during the client initialization and
        the watch cannot be created during that time the method raises
        a ``WatchTimedOut`` exception.

        :param key: key to watch
        :param callback: callback function

        :returns: watch_id. Later it could be used for cancelling watch.
        """
        try:
            return await self.watcher.add_callback(*args, **kwargs)
        except asyncio.QueueEmpty:
            raise exceptions.WatchTimedOut()

    @_handle_errors
    async def watch(self, key, **kwargs):
        """
        Watch a key.

        Example usage:

        .. code-block:: python
            events_iterator, cancel = etcd.watch('/doot/key')
            for event in events_iterator:
                print(event)

        :param key: key to watch

        :returns: tuple of ``events_iterator`` and ``cancel``.
                  Use ``events_iterator`` to get the events of key changes
                  and ``cancel`` to cancel the watch request
        """
        event_queue = asyncio.Queue()

        def callback(event):
            event_queue.put_nowait(event)

        watch_id = await self.add_watch_callback(key, callback, **kwargs)
        canceled = asyncio.Event()

        async def cancel():
            canceled.set()
            await event_queue.put(None)
            await self.cancel_watch(watch_id)

        @_handle_errors
        async def iterator():
            while not canceled.is_set():
                event = await  event_queue.get()
                if event is None:
                    canceled.set()
                if isinstance(event, Exception):
                    canceled.set()
                    raise event
                if not canceled.is_set():
                    yield event

        return iterator(), cancel

    @_handle_errors
    async def watch_prefix(self, key_prefix, **kwargs):
        """Watches a range of keys with a prefix."""
        kwargs['range_end'] = \
            utils.increment_last_byte(utils.to_bytes(key_prefix))
        return await self.watch(key_prefix, **kwargs)

    @_handle_errors
    async def watch_once(self, key, timeout=None, **kwargs):
        """
        Watch a key and stops after the first event.

        If the timeout was specified and event didn't arrived method
        will raise ``WatchTimedOut`` exception.

        :param key: key to watch
        :param timeout: (optional) timeout in seconds.
        :returns: ``Event``
        """
        event_queue = asyncio.Queue()

        def callback(event):
            event_queue.put_nowait(event)

        watch_id = await self.add_watch_callback(key, callback, **kwargs)

        try:
            return await asyncio.wait_for(event_queue.get(), timeout)
        except asyncio.QueueEmpty:
            raise exceptions.WatchTimedOut()
        finally:
            await self.cancel_watch(watch_id)

    @_handle_errors
    async def watch_prefix_once(self, key_prefix, timeout=None, **kwargs):
        """
        Watches a range of keys with a prefix and stops after the first event.

        If the timeout was specified and event didn't arrived method
        will raise ``WatchTimedOut`` exception.
        """
        kwargs['range_end'] = \
            utils.increment_last_byte(utils.to_bytes(key_prefix))
        return await self.watch_once(key_prefix, timeout=timeout, **kwargs)

    @_handle_errors
    async def cancel_watch(self, watch_id):
        """
        Stop watching a key or range of keys.

        :param watch_id: watch_id returned by ``add_watch_callback`` method
        """
        await self.watcher.cancel(watch_id)

    def _ops_to_requests(self, ops):
        """
        Return a list of grpc requests.

        Returns list from an input list of etcd3.transactions.{Put, Get,
        Delete} objects.
        """
        request_ops = []
        for op in ops:
            if isinstance(op, transactions.Put):
                request = self._build_put_request(op.key, op.value, op.lease)
                request_op = RequestOp(request_put=request)
                request_ops.append(request_op)

            elif isinstance(op, transactions.Get):
                request = self._build_get_range_request(op.key)
                request_op = RequestOp(request_range=request)
                request_ops.append(request_op)

            elif isinstance(op, transactions.Delete):
                request = self._build_delete_request(op.key)
                request_op = RequestOp(request_delete_range=request)
                request_ops.append(request_op)

            else:
                raise Exception(
                    'Unknown request class {}'.format(op.__class__))
        return request_ops

    @_handle_errors
    async def transaction(self, compare, success=None, failure=None):
        """
        Perform a transaction.

        Example usage:

        .. code-block:: python

            etcd.transaction(
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

        :param compare: A list of comparisons to make
        :param success: A list of operations to perform if all the comparisons
                        are true
        :param failure: A list of operations to perform if any of the
                        comparisons are false
        :return: A tuple of (operation status, responses)
        """
        compare = [c.build_message() for c in compare]

        success_ops = self._ops_to_requests(success)
        failure_ops = self._ops_to_requests(failure)

        transaction_request = TxnRequest(
            compare=compare,
            success=success_ops,
            failure=failure_ops)
        txn_response = await self.kvstub.Txn(
            transaction_request,
            self.timeout,
            credentials=self.call_credentials,
        )

        responses = []
        for response in txn_response.responses:
            response_type = response.WhichOneof('response')
            if response_type == 'response_put':
                responses.append(None)

            elif response_type == 'response_range':
                range_kvs = []
                for kv in response.response_range.kvs:
                    range_kvs.append((kv.value, KVMetadata(kv)))

                responses.append(range_kvs)

        return txn_response.succeeded, responses

    @_handle_errors
    async def lease(self, ttl, lease_id=None):
        """
        Create a new lease.

        All keys attached to this lease will be expired and deleted if the
        lease expires. A lease can be sent keep alive messages to refresh the
        ttl.

        :param ttl: Requested time to live
        :param lease_id: Requested ID for the lease

        :returns: new lease
        :rtype: :class:`.Lease`
        """
        lease_grant_request = LeaseGrantRequest(TTL=ttl, ID=lease_id)
        lease_grant_response = await self.leasestub.LeaseGrant(
            lease_grant_request,
            self.timeout,
            credentials=self.call_credentials,
        )
        return leases.Lease(lease_id=lease_grant_response.ID,
                            ttl=lease_grant_response.TTL,
                            etcd_client=self)

    @_handle_errors
    async def revoke_lease(self, lease_id):
        """
        Revoke a lease.

        :param lease_id: ID of the lease to revoke.
        """
        lease_revoke_request = LeaseRevokeRequest(ID=lease_id)
        await self.leasestub.LeaseRevoke(
            lease_revoke_request,
            self.timeout,
            credentials=self.call_credentials,
        )

    @_handle_errors
    async def refresh_lease(self, lease_id):
        keep_alive_request = LeaseKeepAliveRequest(ID=lease_id)

        async def refresh_iterator(q):
            while True:
                r = await q.get()
                if r is None:
                    break
                else:
                    yield r

        q = asyncio.Queue()
        result = self.leasestub.LeaseKeepAlive(
            refresh_iterator(q),
            self.timeout,
            credentials=self.call_credentials)
        await q.put(keep_alive_request)
        await result.__anext__()
        await q.put(None)
        await result.aclose()
        try:
            await result.__anext__()
        except StopAsyncIteration:
            pass
        assert result.is_active() is False

    @_handle_errors
    async def get_lease_info(self, lease_id):
        # only available in etcd v3.1.0 and later
        ttl_request = LeaseTimeToLiveRequest(
            ID=lease_id,
            keys=True)
        return await self.leasestub.LeaseTimeToLive(
            ttl_request,
            self.timeout,
            credentials=self.call_credentials,
        )

    @_handle_errors
    def lock(self, name, ttl=60):
        """
        Create a new lock.

        :param name: name of the lock
        :type name: string or bytes
        :param ttl: length of time for the lock to live for in seconds. The
                    lock will be released after this time elapses, unless
                    refreshed
        :type ttl: int
        :returns: new lock
        :rtype: :class:`.Lock`
        """
        return locks.Lock(name, ttl=ttl, etcd_client=self)

    @_handle_errors
    async def add_member(self, urls):
        """
        Add a member into the cluster.

        :returns: new member
        :rtype: :class:`.Member`
        """
        member_add_request = MemberAddRequest(peerURLs=urls)

        member_add_response = await self.clusterstub.MemberAdd(
            member_add_request,
            self.timeout,
            credentials=self.call_credentials,
        )

        member = member_add_response.member
        return aiogrpc_etcd3.members.Member(
            member.ID,
            member.name,
            member.peerURLs,
            member.clientURLs,
            etcd_client=self)

    @_handle_errors
    async def remove_member(self, member_id):
        """
        Remove an existing member from the cluster.

        :param member_id: ID of the member to remove
        """
        member_rm_request = MemberRemoveRequest(ID=member_id)
        await self.clusterstub.MemberRemove(
            member_rm_request,
            self.timeout,
            credentials=self.call_credentials,
        )

    @_handle_errors
    async def update_member(self, member_id, peer_urls):
        """
        Update the configuration of an existing member in the cluster.

        :param member_id: ID of the member to update
        :param peer_urls: new list of peer urls the member will use to
                          communicate with the cluster
        """
        member_update_request = MemberUpdateRequest(
            ID=member_id,
            peerURLs=peer_urls)
        await self.clusterstub.MemberUpdate(
            member_update_request,
            self.timeout,
            credentials=self.call_credentials,
        )

    @property
    async def members(self):
        """
        List of all members associated with the cluster.

        :type: sequence of :class:`.Member`

        """
        member_list_request = MemberListRequest()
        member_list_response = await self.clusterstub.MemberList(
            member_list_request,
            self.timeout,
            credentials=self.call_credentials,
        )

        for member in member_list_response.members:
            yield aiogrpc_etcd3.members.Member(
                member.ID,
                member.name,
                member.peerURLs,
                member.clientURLs,
                etcd_client=self)

    @_handle_errors
    async def compact(self, revision, physical=False):
        """
        Compact the event history in etcd up to a given revision.

        All superseded keys with a revision less than the compaction revision
        will be removed.

        :param revision: revision for the compaction operation
        :param physical: if set to True, the request will wait until the
                         compaction is physically applied to the local database
                         such that compacted entries are totally removed from
                         the backend database
        """
        compact_request = CompactionRequest(
            revision=revision,
            physical=physical)
        await self.kvstub.Compact(
            compact_request,
            self.timeout,
            credentials=self.call_credentials,
        )

    @_handle_errors
    async def defragment(self):
        """Defragment a member's backend database to recover storage space."""
        defrag_request = DefragmentRequest()
        await self.maintenancestub.Defragment(
            defrag_request,
            self.timeout,
            credentials=self.call_credentials,
        )

    @_handle_errors
    async def hash(self):
        """
        Return the hash of the local KV state.

        :returns: kv state hash
        :rtype: int
        """
        hash_request = HashRequest()
        return await self.maintenancestub.Hash(hash_request).hash

    def _build_alarm_request(self, alarm_action, member_id, alarm_type):
        alarm_request = AlarmRequest()

        if alarm_action == 'get':
            alarm_request.action = AlarmRequest.GET
        elif alarm_action == 'activate':
            alarm_request.action = AlarmRequest.ACTIVATE
        elif alarm_action == 'deactivate':
            alarm_request.action = AlarmRequest.DEACTIVATE
        else:
            raise ValueError('Unknown alarm action: {}'.format(alarm_action))

        alarm_request.memberID = member_id

        if alarm_type == 'none':
            alarm_request.alarm = NONE
        elif alarm_type == 'no space':
            alarm_request.alarm = NOSPACE
        else:
            raise ValueError('Unknown alarm type: {}'.format(alarm_type))

        return alarm_request

    @_handle_errors
    async def create_alarm(self, member_id=0):
        """Create an alarm.

        If no member id is given, the alarm is activated for all the
        members of the cluster. Only the `no space` alarm can be raised.

        :param member_id: The cluster member id to create an alarm to.
                          If 0, the alarm is created for all the members
                          of the cluster.
        :returns: list of :class:`.Alarm`
        """
        alarm_request = self._build_alarm_request('activate',
                                                  member_id,
                                                  'no space')
        alarm_response = await self.maintenancestub.Alarm(
            alarm_request,
            self.timeout,
            credentials=self.call_credentials,
        )

        return [Alarm(alarm.alarm, alarm.memberID)
                for alarm in alarm_response.alarms]

    @_handle_errors
    async def list_alarms(self, member_id=0, alarm_type='none'):
        """List the activated alarms.

        :param member_id:
        :param alarm_type: The cluster member id to create an alarm to.
                           If 0, the alarm is created for all the members
                           of the cluster.
        :returns: sequence of :class:`.Alarm`
        """
        alarm_request = self._build_alarm_request('get',
                                                  member_id,
                                                  alarm_type)
        alarm_response = await self.maintenancestub.Alarm(
            alarm_request,
            self.timeout,
            credentials=self.call_credentials,
        )

        for alarm in alarm_response.alarms:
            yield Alarm(alarm.alarm, alarm.memberID)

    @_handle_errors
    async def disarm_alarm(self, member_id=0):
        """Cancel an alarm.

        :param member_id: The cluster member id to cancel an alarm.
                          If 0, the alarm is canceled for all the members
                          of the cluster.
        :returns: List of :class:`.Alarm`
        """
        alarm_request = self._build_alarm_request('deactivate',
                                                  member_id,
                                                  'no space')
        alarm_response = await self.maintenancestub.Alarm(
            alarm_request,
            self.timeout,
            credentials=self.call_credentials,
        )

        return [Alarm(alarm.alarm, alarm.memberID)
                for alarm in alarm_response.alarms]

    @_handle_errors
    async def snapshot(self, file_obj):
        """Take a snapshot of the database.

        :param file_obj: A file-like object to write the database contents in.
        """
        snapshot_request = SnapshotRequest()
        snapshot_response = await self.maintenancestub.Snapshot(
            snapshot_request,
            self.timeout,
            credentials=self.call_credentials,
        )

        for response in snapshot_response:
            file_obj.write(response.blob)


async def client(
        host='localhost', port=2379,
        ca_cert=None, cert_key=None, cert_cert=None, timeout=None,
        user=None, password=None, loop=None):
    """Return an instance of an Etcd3Client."""
    clientobj = Etcd3Client(
        host=host,
        port=port,
        ca_cert=ca_cert,
        cert_key=cert_key,
        cert_cert=cert_cert,
        timeout=timeout,
        user=user,
        password=password,
        loop=loop)
    await clientobj.initialization()
    return clientobj
