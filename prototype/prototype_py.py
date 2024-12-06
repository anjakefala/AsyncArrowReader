from .prototype_cpp import process_stream
import pyarrow as pa
import asyncio
from typing import Optional, AsyncIterator, Union


class AsyncRecordBatchReader:
    """Asynchronous reader for Arrow RecordBatches over IPC."""

    def __init__(self, verbose: bool = False):
        self._queue: asyncio.Queue = asyncio.Queue()
        self._loop = asyncio.get_event_loop()
        self._error: Optional[Exception] = None
        self._verbose: bool = verbose
        self._schema = self._loop.create_future()
        self._result = None

    def _log(self, msg):
        if self._verbose:
            print(msg)

    @property
    async def schema(self):
        """Return the schema of the record batches.

        Raises
        ------
            ValueError: If schema is not yet available
        """
        await asyncio.wait_for(self.schema)

    async def __aiter__(self) -> AsyncIterator[Union[pa.RecordBatch, Exception]]:
        """Iterate over received record batches asynchronously."""
        try:
            while not self._result.done():
                batch = await self._queue.get()
                if isinstance(batch, Exception):
                    raise batch
                if batch is None:
                    break
                yield batch
        finally:
            if self._error:
                raise self._error

    def _handle_batch(self, ptr):
        """Handle incoming record batch from arrow::ipc::StreamDecoder.

        Paramaters
        ----------
            ptr: Pointer to ArrowArrayStream
        """
        self._log(f"Received batch pointer: {ptr}")

        try:
            # Import the record batch from C
            stream = pa.RecordBatchReader._import_from_c(ptr)
            batch = next(stream)

            # Set schema if not already set
            if not self._schema.done():
                self._schema.set_result(batch.schema)
                self._log(f"Schema initialised")

            # Queue the batch for async consumption
            self._loop.call_soon_threadsafe(self._queue.put_nowait, batch)

            self._log(f"Queued batch with {len(batch)} rows")

        except Exception as e:
            self._log(f"Error in callback: {e}")
            self._error = e
            self._loop.call_soon_threadsafe(self._queue.put_nowait, e)


def fetch_stream(url: str, verbose: bool = False) -> AsyncRecordBatchReader:
    """Fetch an Arrow IPC stream from a URL.

    Parameters
    ----------
        url: URL to fetch Arrow IPC stream from

    Returns
    -------
        AsyncRecordBatchReader: Iterator with record batches

    Raises
    ------
        RuntimeError: if stream processing fails
    """
    reader = AsyncRecordBatchReader(verbose=verbose)
    reader._result = asyncio.get_event_loop().run_in_executor(
        None,
        lambda: process_stream(url=url, callback=reader._handle_batch),
    )

    return reader
