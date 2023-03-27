import datetime
import time
import typing

import psycopg2
import psycopg2.pool

from telethon.sessions import SQLiteSession, MemorySession
from telethon.sessions.sqlite import CURRENT_VERSION
from telethon.crypto import AuthKey
from telethon.tl import types


POSTGRES_SQL_VERSION = 7
if POSTGRES_SQL_VERSION != CURRENT_VERSION:
    raise ValueError("Update sqlscripts!")

Values = typing.Optional[
    typing.Union[
        tuple[typing.Any],
        dict[str, typing.Any],
        typing.Iterable[dict[str, typing.Any]],
    ]
]

ExecuteResults = typing.Optional[
    typing.Union[
        list[typing.Any],
        list[list[typing.Any]],
    ]
]


class PostgresqlSession(SQLiteSession):  # type: ignore
    """
    Telethon Session class for store data in postgresql database
    """

    def __init__(
        self,
        dbname: str,
        schema: str,
        username: str,
        password: str,
        host: str = "127.0.0.1",
        port: int = 5432,
        min_pool_connections: int = 1,
        max_pool_connections: int = 10,
    ):

        MemorySession.__init__(self)

        self._pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=min_pool_connections,
            maxconn=max_pool_connections,
            user=username,
            password=password,
            host=host,
            port=port,
            database=dbname,
            options=f"-c search_path={schema}",
        )

        self.save_entities = True

        tables_exist = self.fetchone(
            """
            select
                table_name
            from
                information_schema.tables
            where
                table_schema = %(schema)s
                and table_name = 'version'
            """,
            {"schema": schema},
        )

        if tables_exist:
            self._check_tables_version()
        else:
            self._create_tables()

    def _check_tables_version(self) -> None:
        version_results = self.fetchone("select version from version")

        if not version_results:
            raise ValueError("Version select return nothing")

        version = version_results[0]
        if version < CURRENT_VERSION:
            self._upgrade_database(old=version)
            self.execute("delete from version")
            self.execute("insert into version values (%s)", (CURRENT_VERSION,))
            self.save()

        tuple_ = self.fetchone("select * from sessions")
        if tuple_:
            (
                self._dc_id,
                self._server_address,
                self._port,
                key,
                self._takeout_id,
            ) = tuple_
            self._auth_key = AuthKey(data=key.tobytes())

    def _create_tables(self) -> None:
        self._create_table(
            "version (version integer primary key)",
            """sessions (
                dc_id integer primary key,
                server_address text,
                port integer,
                auth_key bytea,
                takeout_id integer
            )""",
            """entities (
                id integer primary key,
                hash bigint not null,
                username text,
                phone bigint,
                name text,
                date bigint
            )""",
            """sent_files (
                md5_digest bytea,
                file_size integer,
                type integer,
                id integer,
                hash integer,
                primary key(md5_digest, file_size, type)
            )""",
            """update_state (
                id integer primary key,
                pts integer,
                qts integer,
                date integer,
                seq integer
            )""",
        )
        self.execute("insert into version values (%s)", (CURRENT_VERSION,))
        self._update_session_table()

    def save(self) -> None:
        pass

    def _execute(
        self,
        stmt: str,
        values: Values = None,
        fetchone: bool = True,
        fetchall: bool = False,
        executemany: bool = False,
    ) -> ExecuteResults:

        if all((fetchone, fetchall)):
            raise ValueError("Choose fetchall or fetchone")

        connection = self._pool.getconn()
        connection.autocommit = True
        cursor = connection.cursor()

        try:

            if executemany:
                cursor.executemany(stmt, values)
            else:
                cursor.execute(stmt, values)

            results = cursor.rowcount

            if results:
                if fetchall:
                    return cursor.fetchall()  # type: ignore
                elif fetchone:
                    return cursor.fetchone()  # type: ignore
                else:
                    return None
            else:
                return None
        finally:
            cursor.close()
            self._pool.putconn(connection)

    def fetchall(
        self,
        stmt: str,
        values: Values = None,
    ) -> typing.Optional[list[list[typing.Any]]]:
        return self._execute(
            stmt, values, fetchall=True, fetchone=False, executemany=False
        )

    def fetchone(
        self,
        stmt: str,
        values: Values = None,
    ) -> typing.Optional[list[typing.Any]]:
        return self._execute(
            stmt, values, fetchall=False, fetchone=True, executemany=False
        )

    def execute(
        self,
        stmt: str,
        values: Values = None,
    ) -> None:
        self._execute(
            stmt, values, fetchall=False, fetchone=False, executemany=False
        )

    def executemany(
        self,
        stmt: str,
        values: Values = None,
    ) -> ExecuteResults:
        return self._execute(
            stmt, values, fetchall=False, fetchone=False, executemany=True
        )

    def close(self) -> None:
        raise NotImplementedError

    def delete(self) -> None:
        raise NotImplementedError

    @classmethod
    def list_sessions(cls: SQLiteSession) -> None:
        raise NotImplementedError

    def _update_session_table(self) -> None:

        self.execute("delete from sessions")

        # data from MemorySession.__init__
        data = {
            "dc_id": self._dc_id,
            "server": self._server_address,
            "port": self._port,
            "auth_key": self._auth_key.key if self._auth_key else b"",
            "takeout_id": self._takeout_id,
        }

        self.execute(
            """
            insert
                into
                sessions
            values (%(dc_id)s,
            %(server)s,
            %(port)s,
            %(auth_key)s,
            %(takeout_id)s) on
            conflict (dc_id) do
            update
            set
                (server_address,
                port,
                auth_key,
                takeout_id) = (%(server)s,
                %(port)s,
                %(auth_key)s,
                %(takeout_id)s)
            """,
            data,
        )

    def process_entities(self, tlo: typing.Any) -> None:

        print("process_entities", tlo, type(tlo))

        if not self.save_entities:
            return

        rows = self._entities_to_rows(tlo)
        if not rows:
            return

        now_tup = (int(time.time()),)
        rows = [row + now_tup for row in rows]

        rows_as_dict = []
        for row in rows:
            rows_as_dict.append(
                dict(
                    zip(
                        [
                            "id",
                            "hash",
                            "username",
                            "phone",
                            "name",
                            "date",
                        ],
                        row,
                    )
                )
            )

        self.executemany(
            """
            insert
                into
                entities
            values (%(id)s,
            %(hash)s,
            %(username)s,
            %(phone)s,
            %(name)s,
            %(date)s) on
            conflict (id) do
            update
            set
                (id,
                hash,
                username,
                phone,
                name,
                date) = (%(id)s,
                %(hash)s,
                %(username)s,
                %(phone)s,
                %(name)s,
                %(date)s)
            """,
            rows_as_dict,
        )

    def set_dc(self, dc_id: int, server_address: str, port: int) -> None:
        MemorySession.set_dc(self, dc_id, server_address, port)
        self._update_session_table()

        row = self.fetchall("select auth_key from sessions")
        if row and row[0]:
            self._auth_key = AuthKey(data=row[0])
        else:
            self._auth_key = None

    def get_update_states(self) -> typing.Any:
        rows = self.fetchall(
            "select id, pts, qts, date, seq from update_state"
        )

        print("get_update_states", rows)

        if not rows:
            raise ValueError("update_states select return nothing")

        return (
            (
                row[0],
                types.updates.State(
                    pts=row[1],
                    qts=row[2],
                    date=datetime.datetime.fromtimestamp(
                        row[3], tz=datetime.timezone.utc
                    ),
                    seq=row[4],
                    unread_count=0,
                ),
            )
            for row in rows
        )

    def get_entity_rows_by_username(
        self,
        username: str,
    ) -> typing.Optional[tuple[str, str]]:

        results = self.fetchall(
            "select id, hash, date from entities where username = %s",
            (username,),
        )

        if not results:
            return None

        # If there is more than one result for the same username,
        # evict the oldest one

        if len(results) > 1:
            results.sort(key=lambda t: t[2] or 0)
            self.executemany(
                "update entities set username = null where id = %s",
                [(t[0],) for t in results[:-1]],  # type: ignore
            )

        return results[-1][0], results[-1][1]

    def _upgrade_database(self, old: int) -> None:

        if old == 1:
            old += 1
            # old == 1 doesn't have the old sent_files so no need to drop

        if old == 2:
            old += 1
            # Old cache from old sent_files lasts then a day anyway, drop
            self.execute("drop table sent_files")
            self._create_table(
                """sent_files (
                    md5_digest bytea,
                    file_size integer,
                    type integer,
                    id integer,
                    hash integer,
                    primary key(md5_digest, file_size, type)
                )""",
            )

        if old == 3:
            old += 1
            self._create_table(
                """update_state (
                    id integer primary key,
                    pts integer,
                    qts integer,
                    date integer,
                    seq integer
                )""",
            )

        if old == 4:
            old += 1
            self.execute("alter table sessions add column takeout_id integer")

        if old == 5:
            # Not really any schema upgrade, but potentially all access
            # hashes for User and Channel are wrong, so drop them off.
            old += 1
            self.execute("delete from entities")

        if old == 6:
            old += 1
            self.execute("alter table entities add column date integer")

    def _create_table(self, *definitions: typing.Iterable[str]) -> None:
        for definition in definitions:
            self.execute(f"create table {definition}")

