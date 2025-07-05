# zlog-sql
MySQL logging plugin for ZNC IRC bouncer written in Python 3. The current version supports only MySQL databases.

## Features
* Supports MySQL database only.
* Asynchronous database writes on separate thread. Guarantees that ZNC won't hang during SQL connection timeout.
* Automatic table creation (`CREATE TABLE IF NOT EXIST`)
* Retry after failed inserts. When the database server is offline, logs are buffered to memory. They are saved when the database is back online, so you won't lose logs during MySQL outages.

## Some statistics
After having this plugin enabled for around 11 months, below are my statistics of MySQL table:
* Total logs count: more than 4.87 million.
* Space usage: 386 MB (data 270 MB, index 116 MB)

MySQL gives great compression ratio and is easily searchable.

## Installation
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
For MySQL, set module argument matching following format:
```
mysql://username:password@localhost/database_name
```
**Important:** you need [`PyMySQL`](https://github.com/PyMySQL/PyMySQL) pip package for MySQL logging. Install it with `pip3 install PyMySQL` command.

5. Save changes. SQL table schema is going to be created automatically.
