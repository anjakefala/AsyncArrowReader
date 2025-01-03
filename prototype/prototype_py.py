import asyncio
from typing import Optional, AsyncIterator, Union

import pyarrow as pa
import aiohttp

from .prototype_cpp import StreamDecoderWrapper

class AsyncRecordBatchReader:
    """Asynchronous reader for Arrow RecordBatches over IPC."""

    def __init__(self, verbose: bool = False):
        self._queue: asyncio.Queue = asyncio.Queue()    # AsyncIO queue for storing received batches
        self._loop = asyncio.get_event_loop()           # the event loop this reader is running on
        self._error: Optional[Exception] = None         # stores any error that occurred during processing
        self._verbose: bool = verbose                   # if true, prints debug info
        self._schema = self._loop.create_future()       # future that will contain the schema once received
        self._done = False                              # flag indicating if all data has been read

    def _log(self, msg):
        if self._verbose:
            print(msg)

    def mark_done(self):
        """Mark that all data has been read from stream. Indicates no more batches will be received."""
        self._done = True

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
            while not self._done or not self._queue.empty():
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

        Is called by the C++ code when a complete batch is available.
        It converts the C Data Interface pointer to a PyArrow batch and queues it
        for async consumpution.

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

async def _read_stream(url: str, wrapper: StreamDecoderWrapper, reader: AsyncRecordBatchReader):
    """Background task to read the stream

    Parameters
    ----------
        url: URL to fetch Arrow IPC stream from
        wrapper: StreamDecoderWrapper instance to consume bytes
        reader: AsyncRecordBatchReader to receive batches

    Raises
    ------
        Exception: Any error during stream reading is caught and stored in reader
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                buf_size = 8192
                while True:
                    chunk = await response.content.read(buf_size)
                    if not chunk:   # EOF
                        break
                    wrapper.consume_bytes(bytearray(chunk))
        reader.mark_done()
    except Exception as e:
        reader._error = e
        reader.mark_done()
        raise


async def fetch_stream(url: str, verbose: bool = False) -> AsyncRecordBatchReader:
    """Fetch an Arrow IPC stream from a URL asynchronously.

    Starts a background task to read the stream and returns immediately.
    The returned AsyncRecordBatchReader can be used to iterate over batches as they become available.

    Parameters
    ----------
        url: URL to fetch Arrow IPC stream from
        verbose: if True, print debug information about batch processing

    Returns
    -------
        AsyncRecordBatchReader: Iterator with record batches

    Raises
    ------
        RuntimeError: if stream processing fails

    Example
    -------
        >> reader = await fetch_stream("https://example.com/data.arrow")
        >> async for batch in reader:
        ...     process_batch(batch)
    """
    reader = AsyncRecordBatchReader(verbose=verbose)

    wrapper = StreamDecoderWrapper()
    wrapper.set_callback(reader._handle_batch)

    asyncio.create_task(_read_stream(url, wrapper, reader))

    return reader
