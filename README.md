# Cosmos DB Data Export Tool

A CLI tool to export data from Azure Cosmos DB (SQL API) to JSON format, featuring real-time Request Units (RU/s) consumption monitoring.

## Features

- **Parallel Export**: Uses a thread pool to export containers in parallel.
- **Real-time Statistics**: Monitors total RU consumed and the RU/s rate during the operation.
- **Flexibility**: Export all containers or a specific one using CLI arguments.
- **Easy Configuration**: Supports environment variables via `.env` file or command-line options.

## Requirements

- Python 3.14+
- [PDM](https://pdm.fming.dev/) (Package Manager)

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd mongo-dumper
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

You can run the tool using the installed CLI command:
```bash
pdm run mongo-dumper
```

### Command Line Options

You can override the `.env` configuration using arguments:

- `--url`: Cosmos DB endpoint URL.
- `--key`: Primary key.
- `--db`: Database name.
- `--container`: Specific container name to export.

Example for a single container:
```bash
pdm run mongo-dumper --container "my-collection"
```

Show help:
```bash
pdm run mongo-dumper --help
```

## Output

Data is saved in the `export/<database_name>_<timestamp>/` folder as JSON files.

## License

This project is distributed under the MIT license. See the `LICENSE` file for details.
