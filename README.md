# zlog-sql
MySQL logging plugin for ZNC IRC bouncer written in Python 3. It now supports logging to a local SQLite (libSQL) database which can be synced to MySQL/PlanetScale.

## Features
* Supports MySQL or a local SQLite buffer.
* Asynchronous database writes on separate thread. Guarantees that ZNC won't hang during SQL connection timeout.
* Automatic table creation (`CREATE TABLE IF NOT EXISTS`)
* Retry after failed inserts. When the database server is offline, logs are buffered to memory. They are saved when the database is back online, so you won't lose logs during MySQL outages.

## Installation

Clone this repository and copy `zlog_sql.py` to your ZNC modules directory
(usually `~/.znc/modules/`).

If you require MySQL support, install the `PyMySQL` dependency separately:

```bash
pip install PyMySQL
```


## Some statistics
After having this plugin enabled for around 11 months, below are my statistics of MySQL table:
* Total logs count: more than 4.87 million.
* Space usage: 386 MB (data 270 MB, index 116 MB)

MySQL gives great compression ratio and is easily searchable.

## Development setup
Install the Python dependencies with:

```bash
pip install -r requirements.txt
```

## Quick start
1. Copy `zlog_sql.py` to `~/.znc/modules/zlog_sql.py`.
2. In Webadmin, open the list of Global Modules.
3. Make sure `modpython` is enabled.
4. Enable module `zlog_sql` and set its argument.

![Screenshot](docs/webadmin_modules.png)

### MySQL
For MySQL, set module argument using the following format:
```
mysql://username:password@localhost/database_name
```
**Important:** MySQL functionality requires the `PyMySQL` package. Install it
with:

```bash
pip install PyMySQL
```

### SQLite buffer
To log locally and automatically sync every 5 seconds, use a connection string like:
```
sqlite:///path/to/local.db;mysql://user:pass@host/db
```
The module will spawn a subprocess that pushes buffered rows from the local SQLite
file to the remote MySQL instance on a 5 second interval.

5. Save changes. SQL table schema is going to be created automatically.
