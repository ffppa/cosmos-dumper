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


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Cosmos DB Data Export Tool with RU/s tracking"
    )
    parser.add_argument(
        "--url",
        default=os.getenv("COSMOS_EXPORT_URL"),
        help="Cosmos DB URL (default: from COSMOS_EXPORT_URL env)",
    )
    parser.add_argument(
        "--key",
        default=os.getenv("COSMOS_EXPORT_KEY"),
        help="Cosmos DB Key (default: from COSMOS_EXPORT_KEY env)",
    )
    parser.add_argument(
        "--db",
        default=os.getenv("COSMOS_EXPORT_DB_NAME"),
        help="Cosmos DB Database Name (default: from COSMOS_EXPORT_DB_NAME env)",
    )
    parser.add_argument(
        "--container",
        default=os.getenv("COSMOS_EXPORT_CONTAINER_NAME"),
        help="Specific container to export (optional) (default: from COSMOS_EXPORT_CONTAINER_NAME env)",
    )

    args = parser.parse_args()

    print_banner()

    url = args.url
    key = args.key
    database_name = args.db
    target_container = args.container

    if not all([url, key, database_name]):
        logger.error(
            "Missing configuration. Please provide --url, --key, and --db as arguments or set COSMOS_EXPORT_URL, COSMOS_EXPORT_KEY, and COSMOS_EXPORT_DB_NAME environment variables."
        )
        return

    client = CosmosClient(url, credential=key)
    db = client.get_database_client(database_name)

    today = datetime.today().strftime("%Y-%m-%d-%H-%M")
    export_folder = f"export/{database_name}_{today}"
    os.makedirs(export_folder, exist_ok=True)

    # Global stats
    total_ru = 0.0
    start_time = time.time()

    def export_container(container):
        nonlocal total_ru
        container_name = container["id"]

        # Filter logic: if target_container is specified, use it.
        if target_container:
            if container_name != target_container:
                return

        logger.info(f"Starting export for container: {container_name}")
        container_client = db.get_container_client(container_name)

        items = []
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

            for page in query_iterable.by_page():
                items.extend(list(page))

            with open(f"{export_folder}/{container_name}_export.json", "w") as f:
                json.dump(items, f)

            logger.success(
                f"Exported {len(items)} items from {container_name}. RU consumed: {container_ru:.2f}"
            )
        except Exception as e:
            logger.error(f"Error exporting {container_name}: {e}")

    # Use ThreadPoolExecutor for parallel export
    containers = list(db.list_containers())
    logger.info(f"Found {len(containers)} containers. Starting export...")

    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(export_container, container) for container in containers
        ]

        pbar = tqdm(
            as_completed(futures),
            total=len(containers),
            desc="Exporting containers",
            unit=" containers",
        )
        for _ in pbar:
            elapsed = time.time() - start_time
            rus_per_sec = total_ru / elapsed if elapsed > 0 else 0
            pbar.set_postfix(
                {"Total RU": f"{total_ru:.2f}", "RU/s": f"{rus_per_sec:.2f}"}
            )

    logger.info(f"Dump completed. Total RU consumed: {total_ru:.2f}")


if __name__ == "__main__":
    main()
