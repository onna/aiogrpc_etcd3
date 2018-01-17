import asyncio

import aiogrpc_etcd3.events as events
import aiogrpc_etcd3.exceptions as etcd3_exceptions
import aiogrpc_etcd3.proto as etcdrpc
import aiogrpc_etcd3.utils as utils

import grpc


class Watch(object):

    def __init__(self, watch_id, iterator=None, etcd_client=None):
        self.watch_id = watch_id
        self.etcd_client = etcd_client
        self.iterator = iterator

    async def cancel(self):
        await self.etcd_client.cancel_watch(self.watch_id)

    def iterator(self):
        if self.iterator is not None:
            return self.iterator
        else:
            raise


class Watcher(object):

    def __init__(self, watchstub, timeout=None, call_credentials=None):
        self.timeout = timeout
        self._watch_id_callbacks = {}
        self._watch_id_queue = asyncio.Queue()
        self._watch_id_lock = asyncio.Lock()
        self._watch_requests_queue = asyncio.Queue()
        self._watch_response_iterator = watchstub.Watch(
            self._requests_iterator,
            credentials=call_credentials
        )
        self._callback = None

    async def run(self):
        try:
            async for response in self._watch_response_iterator:
                if response.created:
                    self._watch_id_callbacks[response.watch_id] = \
                        self._callback
                    self._watch_id_queue.put_nowait(response.watch_id)

                callback = self._watch_id_callbacks.get(response.watch_id)
                if callback:
                    # The watcher can be safely reused, but adding a new event
                    # to indicate that the revision is already compacted
                    # requires api change which would break all users of this
                    # module. So, raising an exception if a watcher is still
                    # alive. The caller has to create a new client instance to
                    # recover would break all users of this module.
                    if response.compact_revision != 0:
                        callback(etcd3_exceptions.RevisionCompactedError(
                            response.compact_revision))
                        await self.cancel(response.watch_id)
                        continue
                    for event in response.events:
                        callback(events.new_event(event))
        except grpc.RpcError as e:
            await self.stop()
            if self._watch_id_callbacks:
                for callback in self._watch_id_callbacks.values():
                    callback(e)

    @property
    async def _requests_iterator(self):
        while True:
            request, self._callback = await self._watch_requests_queue.get()
            if request is None:
                break
            yield request

    async def add_callback(
            self, key, callback,
            range_end=None,
            start_revision=None,
            progress_notify=False,
            filters=None,
            prev_kv=False):
        async with self._watch_id_lock:
            create_watch = etcdrpc.WatchCreateRequest()
            create_watch.key = utils.to_bytes(key)
            if range_end is not None:
                create_watch.range_end = utils.to_bytes(range_end)
            if start_revision is not None:
                create_watch.start_revision = start_revision
            if progress_notify:
                create_watch.progress_notify = progress_notify
            if filters is not None:
                create_watch.filters = filters
            if prev_kv:
                create_watch.prev_kv = prev_kv
            request = etcdrpc.WatchRequest(create_request=create_watch)
            self._watch_requests_queue.put_nowait((request, callback))
            return await asyncio.wait_for(
                self._watch_id_queue.get(), self.timeout)

    async def cancel(self, watch_id):
        if watch_id is not None:
            self._watch_id_callbacks.pop(watch_id, None)
            cancel_watch = etcdrpc.WatchCancelRequest()
            cancel_watch.watch_id = watch_id
            request = etcdrpc.WatchRequest(cancel_request=cancel_watch)
            self._watch_requests_queue.put_nowait((request, None))

    async def stop(self):
        self._watch_requests_queue.put_nowait((None, None))
        await self._watch_response_iterator.aclose()
