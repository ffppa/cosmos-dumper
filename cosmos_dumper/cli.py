from azure.cosmos import CosmosClient
import json
import os
import time
import argparse
from datetime import datetime
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from loguru import logger
import glob

def setup_logging():
    logger.remove()
    
    log_format = (
        "<white>{time:HH:mm:ss}</white> | "
        "<level>{level: <8}</level> | "
        "<level>{message}</level>"
    )

    logger.add(lambda msg: tqdm.write(msg, end=""), format=log_format, colorize=True, level="INFO")
    
    os.makedirs("logs", exist_ok=True)

    file_format = "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}"
    logger.add("logs/cosmos_dumper_{time:YYYY-MM-DD}.log", format=file_format, level="DEBUG", rotation="10 MB", compression="zip", colorize=False)

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


def export_cmd(args):
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

    client = CosmosClient(url, credential=key)
    db = client.get_database_client(database_name)
    logger.info(f"Connected to database: {database_name}")

    today = datetime.today().strftime("%Y-%m-%d-%H-%M")
    export_folder = f"export/{database_name}_{today}"
    os.makedirs(export_folder, exist_ok=True)

    total_ru = 0.0
    start_time = time.time()

    def export_container(container):
        nonlocal total_ru
        container_name = container["id"]

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

        try:
            query_iterable = container_client.query_items(
                query="SELECT * FROM c",
                enable_cross_partition_query=True,
                response_hook=response_hook,
            )

            extension = "jsonl" if use_jsonl else "json"
            file_path = f"{export_folder}/{container_name}_export.{extension}"

            with open(file_path, "w") as f:
                if not use_jsonl:
                    f.write("[\n")

                for page in query_iterable.by_page():
                    for i, item in enumerate(page):
                        if item_count > 0:
                            if use_jsonl:
                                f.write("\n")
                            else:
                                f.write(",\n")
                        
                        f.write(json.dumps(item))
                        item_count += 1

                if not use_jsonl:
                    f.write("\n]")

            logger.success(
                f"Exported {item_count} items from [{container_name}]. RU: {container_ru:.2f}"
            )
        except Exception as e:
            logger.error(f"Error exporting {container_name}: {e}")

    containers = list(db.list_containers())
    logger.info(f"Found {len(containers)} containers. Starting export...")

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(export_container, c) for c in containers]
        pbar = tqdm(as_completed(futures), total=len(containers), desc="Exporting", unit="container")
        for _ in pbar:
            elapsed = time.time() - start_time
            rus_per_sec = total_ru / elapsed if elapsed > 0 else 0
            pbar.set_postfix({"Total RU": f"{total_ru:.2f}", "RU/s": f"{rus_per_sec:.2f}"})

    logger.info(f"Export completed. Total RU consumed: {total_ru:.2f}")


def import_cmd(args):
    url = args.url
    key = args.key
    database_name = args.db
    path = args.path
    target_container = args.container
    from_container = getattr(args, "from_container", None)

    if not all([url, key, database_name, path]):
        logger.error("Missing configuration. Please provide --url, --key, --db and --path.")
        return

    client = CosmosClient(url, credential=key)
    db = client.get_database_client(database_name)

    total_ru = 0.0
    start_time = time.time()

    files_to_import = []
    if os.path.isfile(path):
        container_name = target_container or os.path.basename(path).split("_export")[0]
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

    def import_file(container_name, file_path):
        nonlocal total_ru
        logger.info(f"Starting import for container: {container_name} from {file_path}")
        container_client = db.get_container_client(container_name)
        
        try:
            db.create_container_if_not_exists(id=container_name, partition_key={"paths": ["/id"], "kind": "Hash"})
            logger.debug(f"Container {container_name} verified/created.")
        except Exception as e:
            logger.warning(f"Could not verify/create container {container_name}: {e}")

        def response_hook(headers, properties):
            nonlocal total_ru
            charge = float(headers.get("x-ms-request-charge", 0))
            total_ru += charge

        items_imported = 0
        try:
            is_jsonl = file_path.endswith(".jsonl")
            
            def get_items():
                if is_jsonl:
                    with open(file_path, "r") as f:
                        for line in f:
                            if line.strip():
                                yield json.loads(line)
                else:
                    with open(file_path, "r") as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            for item in data:
                                yield item
                        else:
                            yield data

            for item in get_items():
                try:
                    container_client.upsert_item(item, response_hook=response_hook)
                    items_imported += 1
                except Exception as e:
                    logger.error(f"Error importing item in {container_name}: {e}")

            logger.success(f"Imported {items_imported} items into [{container_name}]")
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")

    logger.info(f"Found {len(files_to_import)} files to import. Starting...")

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(import_file, c, f) for c, f in files_to_import]
        pbar = tqdm(as_completed(futures), total=len(files_to_import), desc="Importing", unit="container")
        for _ in pbar:
            elapsed = time.time() - start_time
            rus_per_sec = total_ru / elapsed if elapsed > 0 else 0
            pbar.set_postfix({"Total RU": f"{total_ru:.2f}", "RU/s": f"{rus_per_sec:.2f}"})

    logger.info(f"Import completed. Total RU consumed: {total_ru:.2f}")


def main():
    load_dotenv()
    setup_logging()
    print_banner()

    parser = argparse.ArgumentParser(description="Cosmos DB Data Export/Import Tool")
    subparsers = parser.add_subparsers(dest="command", help="Sub-commands")

    # Common arguments
    def add_common_args(p):
        p.add_argument("--url", default=os.getenv("COSMOS_EXPORT_URL"), help="Cosmos DB URL")
        p.add_argument("--key", default=os.getenv("COSMOS_EXPORT_KEY"), help="Cosmos DB Key")
        p.add_argument("--db", default=os.getenv("COSMOS_EXPORT_DB_NAME"), help="Cosmos DB Database Name")
        p.add_argument("--container", default=os.getenv("COSMOS_EXPORT_CONTAINER_NAME"), help="Specific container name")

    # Export Subparser
    parser_export = subparsers.add_parser("export", help="Export data from Cosmos DB")
    add_common_args(parser_export)
    parser_export.add_argument("--jsonl", action="store_true", help="Export in JSON Lines format")
    parser_export.set_defaults(func=export_cmd)

    # Import Subparser
    parser_import = subparsers.add_parser("import", help="Import data to Cosmos DB")
    add_common_args(parser_import)
    parser_import.add_argument("--path", required=True, help="Path to file or directory to import")
    parser_import.add_argument("--from-container", help="Pick only this container name from directory for import (use with --container to rename)")
    parser_import.set_defaults(func=import_cmd)

    args = parser.parse_args()

    if args.command:
        args.func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
