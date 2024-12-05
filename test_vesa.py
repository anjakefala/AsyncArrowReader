import prototype
import pyarrow as pa
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, AsyncIterator, Union

class AsyncRecordBatchReader:
    """Asynchronous reader for Arrow RecordBatches over IPC."""
    def __init__(self):
        self._schema: Optional[pa.Schema] = None
        self._queue: asyncio.Queue = asyncio.Queue()
        self._loop = asyncio.get_event_loop()
        self._error: Optional[Exception] = None


    @property
    def schema(self):
        """Return the schema of the record batches.

        Raises
        ------
            ValueError: If schema is not yet available
        """
        if self._schema is None:
            raise ValueError("Schema not yet available")
        return self._schema

    async def __aiter__(self) -> AsyncIterator[Union[pa.RecordBatch, Exception]]:
        """Iterate over received record batches asynchronously."""
        try:
            while not self._queue.empty():
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
        print(f"Received batch pointer: {ptr}")

        try:

            # Import the record batch from C
            stream = pa.RecordBatchReader._import_from_c(ptr)
            batch = next(stream)

            # Set schema if not already set
            if self._schema is None:
                self._schema = batch.schema
                print(f"Schema initialised: {self._schema}")

            # Queue the batch for async consumption
            self._loop.call_soon_threadsafe(
                    self._queue.put_nowait, batch
            )

            print(f"Queued batch with {len(batch)} rows")

        except Exception as e:
            print(f"Error in callback: {e}")
            self._error = e
            self._loop.call_soon_threadsafe(
                    self._queue.put_nowait, e
            )

class AsyncArrowClient:
    """Asynchronous client for fetching Arrow Streams."""
    def __init__(self):
        self._reader = AsyncRecordBatchReader()

    async def fetch_stream(self, url: str) -> AsyncRecordBatchReader:
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
        with ThreadPoolExecutor(max_workers=1) as executor:
            try:
                await asyncio.get_event_loop().run_in_executor(
                        executor,
                        lambda: prototype.process_stream(
                            url = url,
                            callback = self._reader._handle_batch
                        )
                    )
            except Exception as e:
                raise RuntimeError(f"Stream processing failed: {e}") from e
        return self._reader


async def main() -> None:
    """Example usage of AsyncArrowClient."""
    client = AsyncArrowClient()
    url = "https://github.com/apache/arrow-experiments/raw/refs/heads/main/data/arrow-commits/arrow-commits.arrows"
    try:
        print(f"Fetching Arrow stream from {url}")
        reader = await client.fetch_stream(url)
        batch_count = 0
        row_count = 0

        async for batch in reader:
            batch_count += 1
            row_count += len(batch)
            print(f"Processed batch {batch_count} with {len(batch)} rows")
    except Exception as e:
        print(f"Error during stream processing: {e}")

    print(prototype.arrow_version())

def test_client():
    asyncio.run(main())

if __name__ == "__main__":
    test_client()
