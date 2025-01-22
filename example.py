import asyncio

from prototype import fetch_stream


async def main() -> None:
    """Example usage of AsyncArrowClient."""
    url = "https://github.com/apache/arrow-experiments/raw/refs/heads/main/data/arrow-commits/arrow-commits.arrows"
    print(f"Fetching Arrow stream from {url}")
    reader = await fetch_stream(url, verbose=True)

    schema = await reader.schema
    print(f"Received schema: {schema}")

    batch_count = 0
    row_count = 0

    print("Starting async iteration")
    async for batch in reader:
        batch_count += 1
        row_count += len(batch)
        print(f"Processed batch {batch_count} with {len(batch)} rows")
        await asyncio.sleep(0.01)   # artificially slow down processing to illustrate async reading + writing
        print(f"Finished processing batch {batch_count}")
    print("Async iteration done")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
