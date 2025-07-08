import argparse
import sqlite3
import pymysql
from urllib.parse import urlparse


def parse_mysql_dsn(dsn: str) -> dict:
    p = urlparse(dsn)
    if p.scheme != 'mysql':
        raise ValueError('Only mysql DSN supported')
    return {
        'host': p.hostname,
        'user': p.username,
        'passwd': p.password,
        'db': p.path.lstrip('/')
    }


def main():
    parser = argparse.ArgumentParser(
        description='Sync local libSQL buffer to PlanetScale (MySQL).'
    )
    parser.add_argument('--sqlite', required=True,
                        help='Path to local libSQL database')
    parser.add_argument('--mysql', required=True,
                        help='MySQL connection string mysql://user:pass@host/db')
    args = parser.parse_args()

    local = sqlite3.connect(args.sqlite)
    local.row_factory = sqlite3.Row
    mysql_dsn = parse_mysql_dsn(args.mysql)
    remote = pymysql.connect(use_unicode=True, charset='utf8mb4', **mysql_dsn)

    with local, remote:
        cur_local = local.cursor()
        cur_remote = remote.cursor()
        rows = cur_local.execute('SELECT rowid as id, * FROM logs').fetchall()
        for row in rows:
            cur_remote.execute(
                'INSERT INTO logs (created_at, user, network, window, type, nick, message) '
                'VALUES (%s, %s, %s, %s, %s, %s, %s)',
                (
                    row['created_at'], row['user'], row['network'],
                    row['window'], row['type'], row['nick'], row['message']
                )
            )
            remote.commit()
            cur_local.execute('DELETE FROM logs WHERE rowid=?', (row['id'],))
            local.commit()


if __name__ == '__main__':
    main()
