# Cosmos DB Data Import/Export Tool

A high-performance CLI tool to import/export data from Azure Cosmos DB (SQL API) using JSON/JSONL format. Designed for speed, scalability, and extreme memory efficiency, it features real-time Request Units (RU/s) monitoring, multi-core processing, and streaming parsers.

## Features

- **Multi-Process Parallelism**: Leverages multiple CPU cores using `pathos.multiprocessing` to process multiple containers or files simultaneously.
- **Hybrid Architecture**: Combines multi-processing with `asyncio` for non-blocking I/O, saturating available network bandwidth and Cosmos DB RU/s.
- **Extreme Memory Efficiency**: Uses `ijson` for streaming JSON parsing. Can handle massive datasets with constant, low RAM usage (few hundred MBs).
- **Smart Export Splitting**: Automatically splits large container exports into multiple files (default 20GB) to enable high-speed parallel importing later.
- **Indexing Policy Optimization**: Automatically disables indexing (`indexingMode: none`) during import to maximize throughput and reduce RU consumption, restoring the original policy upon completion.
- **Streaming Shuffle**: Includes a "sliding window" shuffle buffer during import to distribute write load across all physical partitions of Cosmos DB, preventing hotspots.
- **Real-time Statistics**: Monitors total RU consumed and calculates live RU/s rates.
- **Format Flexibility**: Supports both standard JSON (arrays) and JSON Lines (`.jsonl`) for both import and export.

## Requirements

- Python 3.14+
- [PDM](https://pdm.fming.dev/) (Package Manager) or PIP 25+

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd cosmos-dumper
   ```

2. Install dependencies with PDM:
   ```bash
   pdm install
   ```
3. (OR) Install with PIP
   ```bash
   pip install .    
   ```

## Configuration

Create a `.env` file in the project root:
```bash
cp .env.example .env
```

Available variables:
- `COSMOS_EXPORT_URL`: Your Cosmos DB endpoint.
- `COSMOS_EXPORT_KEY`: Primary access key.
- `COSMOS_EXPORT_DB_NAME`: Database name.

## Usage

### 1. Exporting Data

```bash
pdm run cosmos-dumper export [options]
```

#### Export Options:
- `--url`, `--key`, `--db`: Database credentials (or use `.env`).
- `--container`: Export only this specific container.
- `--jsonl`: Export in JSON Lines format (faster, one object per line).
- `--workers`: Number of parallel processes (default: number of CPUs).
- `--max-file-size`: Max file size in GB before splitting (default: 20).

Example:
```bash
# High-speed export of all containers to JSONL
pdm run cosmos-dumper export --jsonl --workers 8
```

### 2. Importing Data

```bash
pdm run cosmos-dumper import --path <file_or_directory> [options]
```

#### Import Options:
- `--path`: Path to a specific file or a directory containing exported files.
- `--url`, `--key`, `--db`: Database credentials.
- `--container`: Target container name.
- `--from-container`: Filter a specific container from a directory of exports.
- `--workers`: Number of parallel files to process (default: number of CPUs).
- `--concurrency`: Number of concurrent upserts per worker (default: 200).
- `--shuffle`: Enable streaming shuffle to distribute load across partitions.

Example:
```bash
# Massive parallel import with shuffling and high concurrency
pdm run cosmos-dumper import --path ./export/my_dump --workers 4 --concurrency 300 --shuffle
```

## Performance Tips

To achieve maximum performance (GBs per minute):
1. **Scale RU/s**: Temporarily increase your Cosmos DB container RU/s (e.g., to 10k-50k) before starting the import.
2. **Use Shuffle**: Always use `--shuffle` for large datasets to avoid hitting a single physical partition bottleneck.
3. **Tune Concurrency**: Increase `--concurrency` (e.g., 500+) if you have high RU/s and a fast network.
4. **JSONL**: Prefer `--jsonl` during export for simpler streaming and slightly better performance.

## Output Structure

Data is saved in `export/<database_name>_<timestamp>/`.
Files are named:
- `{container}_export.json` (for single files)
- `{container}_export_{index}.json` (for split files)

## License

MIT
