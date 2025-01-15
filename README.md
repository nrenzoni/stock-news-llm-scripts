Includes:
* scripts (and their backing libraries) for ETL between MongoDB, DuckDB, and ClickhouseDB. 
* script for client to request textual embedding from a server (see other github repo).
* OHLCV downloader

Steps to run:
1) copy config-template.toml to config.toml and fill in the necessary fields.
2) from root dir, run `uv run scripts/__` (insert script name) to run that script.