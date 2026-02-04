# Cosmos DB Data Import/Export Tool

A CLI tool to import/export data from Azure Cosmos DB (SQL API) to JSON format, featuring real-time Request Units (RU/s) consumption monitoring and memory-efficient streaming.

## Features

- **Parallel Import/Export**: Uses a thread pool to import/export containers in parallel.
- **Real-time Statistics**: Monitors total RU consumed and the RU/s rate during the operation.
- **Memory Efficient**: Streams data directly to disk to handle large datasets (800GB+) without high RAM usage.
- **JSON Lines Support**: Option to export in `.jsonl` format for better performance with large data.
- **Flexibility**: Export all containers or a specific one using CLI arguments.
- **Easy Configuration**: Supports environment variables via `.env` file or command-line options.

## Requirements

- Python 3.14+
- [PDM](https://pdm.fming.dev/) (Package Manager)

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd cosmos-dumper
   ```

2. Install dependencies and the project with PDM:
   ```bash
   pdm install
   ```
   **or**
   ```bash
   pip install .
   ```

## Configuration

Create a `.env` file in the project root from the template:
```bash
cp .env.example .env
```

Edit the `.env` file with your details:
- `COSMOS_EXPORT_URL`: Your Cosmos DB endpoint.
- `COSMOS_EXPORT_KEY`: Primary access key.
- `COSMOS_EXPORT_DB_NAME`: Database name to export.
- `COSMOS_EXPORT_CONTAINER_NAME`: (Optional) Default specific container.

## Usage

The tool is organized into two main sub-commands: `export` and `import`.

### 1. Exporting Data

Export data from Azure Cosmos DB to JSON/JSONL files.

```bash
pdm run cosmos-dumper export [options]
```

#### Export Options:
- `--url`: Cosmos DB endpoint URL.
- `--key`: Primary key.
- `--db`: Database name.
- `--container`: Specific container name to export.
- `--jsonl`: Use JSON Lines format (highly recommended for large datasets).

Example:
```bash
pdm run cosmos-dumper export --jsonl
```

### 2. Importing Data

Import data from JSON/JSONL files back into Azure Cosmos DB.

```bash
pdm run cosmos-dumper import --path <file_or_directory> [options]
```

#### Import Options:
- `--path`: Path to a specific `.json`/`.jsonl` file or a directory containing exported files.
- `--url`, `--key`, `--db`: Database credentials.
- `--container`: Target container name. If importing a single file, this renames the container in Cosmos DB. If importing a directory, it filters the files.
- `--from-container`: (Optional) Use when importing from a directory to select a specific container and optionally rename it using `--container`.

Example (Import and rename):
```bash
pdm run cosmos-dumper import --path ./export/my_db/old_container_export.json --container new_container_name
```

Example (Import specific container from directory and rename it):
```bash
pdm run cosmos-dumper import --path ./export/my_db/ --from-container old_name --container new_name
```

### Common Configuration

You can still use a `.env` file or environment variables to avoid passing credentials every time:
- `COSMOS_EXPORT_URL`
- `COSMOS_EXPORT_KEY`
- `COSMOS_EXPORT_DB_NAME`

Show help for any command:
```bash
pdm run cosmos-dumper --help
pdm run cosmos-dumper export --help
pdm run cosmos-dumper import --help
```

## Output

Data is saved in the `export/<database_name>_<timestamp>/` folder as JSON files.

## License

This project is distributed under the MIT license. See the `LICENSE` file for details.
