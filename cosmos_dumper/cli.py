from azure.cosmos.aio import CosmosClient
import asyncio
import json
import os
import time
import argparse
from datetime import datetime
from dotenv import load_dotenv
from tqdm.asyncio import tqdm
from loguru import logger
import glob
import aiofiles
import random
import ijson


def setup_logging():
    logger.remove()

    log_format = (
        "<white>{time:HH:mm:ss}</white> | "
        "<level>{level: <8}</level> | "
        "<level>{message}</level>"
    )

    logger.add(
        lambda msg: tqdm.write(msg, end=""),
        format=log_format,
        colorize=True,
        level="INFO",
    )

    os.makedirs("logs", exist_ok=True)

    file_format = "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}"
    logger.add(
        "logs/cosmos_dumper_{time:YYYY-MM-DD}.log",
        format=file_format,
        level="DEBUG",
        rotation="10 MB",
        compression="zip",
        colorize=False,
    )


def print_banner():
    banner = r"""
    ================================================================
        __   ___   _____ ___ ___   ___   _____     ___    __ __  ___ ___  ____   ___  ____  
       /  ] /   \ / ___/|   |   | /   \ / ___/    |   \  |  |  ||   |   ||    \ /  _]|    \ 
      /  / |     (   \_ | _   _ ||     (   \_     |    \ |  |  || _   _ ||  o  )  [_ |  D  )
     /  /  |  O  |\__  ||  \_/  ||  O  |\__  |    |  D  ||  |  ||  \_/  ||   _/    _]|    / 
    /   \_ |     |/  \ ||   |   ||     |/  \ |    |     ||  :  ||   |   ||  | |   [_ |    \ 
    \     ||     |\    ||   |   ||     |\    |    |     ||     ||   |   ||  | |     ||  .  \
     \____| \___/  \___||___|___| \___/  \___|    |_____| \__,_||___|___||__| |_____||__|\_|
     
    by Infra ♡                                                                                                                                                               
    ================================================================
    """
    print(banner)


async def export_cmd(args):
    url = args.url
    key = args.key
    database_name = args.db
    target_container = args.container
    use_jsonl = args.jsonl

    if not all([url, key, database_name]):
        logger.error(
            "Missing configuration. Please provide --url, --key, and --db or set environment variables."
        )
        return

    async with CosmosClient(url, credential=key) as client:
        db = client.get_database_client(database_name)
        logger.info(f"Connected to database: {database_name}")

        today = datetime.today().strftime("%Y-%m-%d-%H-%M")
        export_folder = f"export/{database_name}_{today}"
        os.makedirs(export_folder, exist_ok=True)

        total_ru = 0.0
        start_time = time.time()

        async def writer_worker(queue, file_path, use_jsonl):
            async with aiofiles.open(file_path, "w") as f:
                if not use_jsonl:
                    await f.write("[\n")

                first_item = True
                while True:
                    batch = await queue.get()
                    if batch is None:
                        break

                    for item in batch:
                        if not first_item:
                            if use_jsonl:
                                await f.write("\n")
                            else:
                                await f.write(",\n")

                        await f.write(json.dumps(item))
                        first_item = False

                    queue.task_done()

                if not use_jsonl:
                    await f.write("\n]")
            queue.task_done()

        async def export_container(container_properties):
            nonlocal total_ru
            container_name = container_properties["id"]

            if target_container and container_name != target_container:
                return

            logger.info(f"Starting export for container: {container_name}")
            container_client = db.get_container_client(container_name)

            item_count = 0
            container_ru = 0.0

            def response_hook(headers, properties):
                nonlocal container_ru, total_ru
                charge = float(headers.get("x-ms-request-charge", 0))
                container_ru += charge
                total_ru += charge

            extension = "jsonl" if use_jsonl else "json"
            file_path = f"{export_folder}/{container_name}_export.{extension}"

            queue = asyncio.Queue(maxsize=100)
            writer_task = asyncio.create_task(
                writer_worker(queue, file_path, use_jsonl)
            )

            async def fetch_range(feed_range):
                range_count = 0
                try:
                    query_iterable = container_client.query_items(
                        query="SELECT * FROM c",
                        max_item_count=-1,
                        feed_range=feed_range,
                        response_hook=response_hook,
                    )

                    async for page in query_iterable.by_page():
                        items = [item async for item in page]
                        if items:
                            await queue.put(items)
                            range_count += len(items)
                except Exception as e:
                    logger.error(f"Error fetching range for {container_name}: {e}")
                return range_count

            try:
                # Get partition ranges (Feed Ranges) for parallelization
                pk_ranges = [r async for r in container_client.read_feed_ranges()]
                logger.info(
                    f"Container {container_name} has {len(pk_ranges)} partition ranges. Parallelizing..."
                )

                # Launch all range workers in parallel
                range_tasks = [fetch_range(r) for r in pk_ranges]
                results = await asyncio.gather(*range_tasks)
                item_count = sum(results)

                await queue.put(None)
                await writer_task

                logger.success(
                    f"Exported {item_count} items from [{container_name}]. RU: {container_ru:.2f}"
                )
            except Exception as e:
                logger.error(f"Error exporting {container_name}: {e}")
                writer_task.cancel()

        containers = []
        async for c in db.list_containers():
            containers.append(c)

        logger.info(f"Found {len(containers)} containers. Starting export...")

        tasks = [export_container(c) for c in containers]

        pbar = tqdm(total=len(containers), desc="Exporting", unit="container")

        for coro in asyncio.as_completed(tasks):
            await coro
            pbar.update(1)
            elapsed = time.time() - start_time
            rus_per_sec = total_ru / elapsed if elapsed > 0 else 0
            pbar.set_postfix(
                {"Total RU": f"{total_ru:.2f}", "RU/s": f"{rus_per_sec:.2f}"}
            )

        pbar.close()

    logger.info(f"Export completed. Total RU consumed: {total_ru:.2f}")


async def import_cmd(args):
    url = args.url
    key = args.key
    database_name = args.db
    path = args.path
    target_container = args.container
    from_container = getattr(args, "from_container", None)

    if not all([url, key, database_name, path]):
        logger.error(
            "Missing configuration. Please provide --url, --key, --db and --path."
        )
        return

    async with CosmosClient(url, credential=key) as client:
        db = client.get_database_client(database_name)

        total_ru = 0.0
        start_time = time.time()

        files_to_import = []
        if os.path.isfile(path):
            container_name = (
                target_container or os.path.basename(path).split("_export")[0]
            )
            files_to_import.append((container_name, path))
        elif os.path.isdir(path):
            pattern = os.path.join(path, "*_export.*")
            for f in glob.glob(pattern):
                if f.endswith((".json", ".jsonl")):
                    original_container_name = os.path.basename(f).split("_export")[0]

                    if from_container:
                        if original_container_name != from_container:
                            continue
                        container_name = target_container or original_container_name
                    elif target_container:
                        if original_container_name != target_container:
                            continue
                        container_name = original_container_name
                    else:
                        container_name = original_container_name

                    files_to_import.append((container_name, f))

        if not files_to_import:
            logger.warning(f"No files found to import in {path}")
            return

        semaphore = asyncio.Semaphore(args.concurrency)  # Limit concurrent upserts

        async def import_file(container_name, file_path):
            nonlocal total_ru
            logger.info(
                f"Starting import for container: {container_name} from {file_path}"
            )
            container_client = db.get_container_client(container_name)

            try:
                await db.create_container_if_not_exists(
                    id=container_name,
                    partition_key={"paths": ["/id"], "kind": "Hash"},
                    indexing_policy={"indexingMode": "none"},
                )
                logger.debug(f"Container {container_name} verified/created with indexingMode: none.")
            except Exception as e:
                logger.warning(
                    f"Could not verify/create container {container_name}: {e}"
                )

            # Keep track of original indexing policy to restore it later
            original_indexing_policy = None
            try:
                properties = await container_client.get_container_properties()
                original_indexing_policy = properties.get("indexingPolicy")
            except Exception as e:
                logger.warning(f"Could not retrieve properties for {container_name}: {e}")

            def response_hook(headers, properties):
                nonlocal total_ru
                charge = float(headers.get("x-ms-request-charge", 0))
                total_ru += charge

            items_imported = 0

            async def upsert_with_semaphore(item):
                nonlocal items_imported
                async with semaphore:
                    try:
                        await container_client.upsert_item(
                            item, response_hook=response_hook
                        )
                        items_imported += 1
                    except Exception as e:
                        logger.error(f"Error importing item in {container_name}: {e}")


            try:
                active_tasks = set()
                shuffle_buffer = []
                SHUFFLE_BUFFER_SIZE = 5000

                async def process_item(item_to_process):
                    task = asyncio.create_task(upsert_with_semaphore(item_to_process))
                    active_tasks.add(task)
                    task.add_done_callback(active_tasks.discard)
                    if len(active_tasks) >= args.concurrency + 20:
                        await asyncio.wait(active_tasks, return_when=asyncio.FIRST_COMPLETED)

                with open(file_path, "rb") as f:
                    logger.info(f"Processing {file_path} using ijson streaming parser...")
                    
                    first_char = ""
                    while not first_char:
                        chunk = f.read(1)
                        if not chunk: break
                        if not chunk.isspace():
                            first_char = chunk.decode(errors='ignore')
                    f.seek(0)

                    if first_char == "[":
                        items = ijson.items(f, "item")
                    else:
                        items = ijson.items(f, "", multiple_values=True)

                    for item in items:
                        if args.shuffle:
                            shuffle_buffer.append(item)
                            if len(shuffle_buffer) >= SHUFFLE_BUFFER_SIZE:
                                idx = random.randrange(len(shuffle_buffer))
                                item_to_send = shuffle_buffer.pop(idx)
                                await process_item(item_to_send)
                        else:
                            await process_item(item)
                
                # Flush shuffle buffer
                if shuffle_buffer:
                    random.shuffle(shuffle_buffer)
                    for item in shuffle_buffer:
                        await process_item(item)

                if active_tasks:
                    await asyncio.gather(*active_tasks)

                logger.success(
                    f"Imported {items_imported} items into [{container_name}]"
                )

                if original_indexing_policy and original_indexing_policy.get("indexingMode") != "none":
                    logger.info(f"Restoring indexing policy for {container_name}...")
                    try:
                        await db.replace_container(container_name, original_indexing_policy)
                        logger.success(f"Indexing policy restored for {container_name}")
                    except Exception as e:
                        logger.error(f"Failed to restore indexing policy for {container_name}: {e}")
            except Exception as e:
                logger.error(f"Error reading file {file_path}: {e}")

        logger.info(f"Found {len(files_to_import)} files to import. Starting sequentially...")

        pbar = tqdm(total=len(files_to_import), desc="Importing", unit="container")

        for container_name, file_path in files_to_import:
            await import_file(container_name, file_path)
            pbar.update(1)
            elapsed = time.time() - start_time
            rus_per_sec = total_ru / elapsed if elapsed > 0 else 0
            pbar.set_postfix(
                {"Total RU": f"{total_ru:.2f}", "RU/s": f"{rus_per_sec:.2f}"}
            )

        pbar.close()

    logger.info(f"Import completed. Total RU consumed: {total_ru:.2f}")


def main():
    load_dotenv()
    setup_logging()
    print_banner()

    parser = argparse.ArgumentParser(description="Cosmos DB Data Export/Import Tool")
    subparsers = parser.add_subparsers(dest="command", help="Sub-commands")

    # Common arguments
    def add_common_args(p):
        p.add_argument(
            "--url", default=os.getenv("COSMOS_EXPORT_URL"), help="Cosmos DB URL"
        )
        p.add_argument(
            "--key", default=os.getenv("COSMOS_EXPORT_KEY"), help="Cosmos DB Key"
        )
        p.add_argument(
            "--db",
            default=os.getenv("COSMOS_EXPORT_DB_NAME"),
            help="Cosmos DB Database Name",
        )
        p.add_argument(
            "--container",
            default=os.getenv("COSMOS_EXPORT_CONTAINER_NAME"),
            help="Specific container name",
        )

    # Export Subparser
    parser_export = subparsers.add_parser("export", help="Export data from Cosmos DB")
    add_common_args(parser_export)
    parser_export.add_argument(
        "--jsonl", action="store_true", help="Export in JSON Lines format"
    )
    parser_export.set_defaults(func=lambda args: asyncio.run(export_cmd(args)))

    # Import Subparser
    parser_import = subparsers.add_parser("import", help="Import data to Cosmos DB")
    add_common_args(parser_import)
    parser_import.add_argument(
        "--path", required=True, help="Path to file or directory to import"
    )
    parser_import.add_argument(
        "--from-container",
        help="Pick only this container name from directory for import (use with --container to rename)",
    )
    parser_import.add_argument(
        "--concurrency",
        type=int,
        default=200,
        help="Number of concurrent upserts (default: 200)",
    )
    parser_import.add_argument(
        "--shuffle", action="store_true", help="Shuffle items before importing to distribute load"
    )
    parser_import.set_defaults(func=lambda args: asyncio.run(import_cmd(args)))

    args = parser.parse_args()

    if args.command:
        args.func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
