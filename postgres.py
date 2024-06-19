import datetime
import time
import typing

import psycopg2
import psycopg2.pool
from psycopg2.extensions import AsIs
from telethon.client.telegrambaseclient import (
    DEFAULT_DC_ID,
    DEFAULT_IPV4_IP,
    DEFAULT_PORT,
)
from telethon.crypto import AuthKey
from telethon.sessions import MemorySession, SQLiteSession
from telethon.sessions.sqlite import CURRENT_VERSION
from telethon.tl import types

from config import settings


POSTGRES_SQL_VERSION = 7
if POSTGRES_SQL_VERSION != CURRENT_VERSION:
    raise ValueError("Update sqlscripts!")

Values = typing.Optional[
    typing.Union[
        list[tuple[typing.Any]],
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

    _service_schema_names = [
        "public",
        "information_schema",
        "pg_catalog",
        "pg_toast",
    ]

    def __init__(self, user_schema: str):
        MemorySession.__init__(self)

        if user_schema in self._service_schema_names:
            raise ValueError("Using service schema names is prohibited")

        self._user_schema = user_schema
        self._create_schema(self._user_schema)

        self._pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=settings.min_pool_connections,
            maxconn=settings.max_pool_connections,
            user=settings.db_username,
            password=settings.db_password,
            host=settings.db_host,
            port=settings.db_port,
            database=settings.db_name,
            options=f"-c search_path={self._user_schema}",
        )
        self._conn = None

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
            {"schema": self._user_schema},
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
            self._auth_key = AuthKey(data=bytes(key))

    def _create_tables(self) -> None:
        self._create_table(
            "version (version integer primary key)",
            """sessions (
                dc_id integer primary key,
                server_address text,
                port integer,
                auth_key bytea,
                takeout_id bigint
            )""",
            """entities (
                id bigint primary key,
                hash numeric(25,0) not null,
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
                id bigint primary key,
                pts integer,
                qts integer,
                date bigint,
                seq integer
            )""",
        )
        self.execute("insert into version values (%s)", (CURRENT_VERSION,))
        self._update_session_table()

    def save(self) -> None:
        pass

    def fetchall(
        self,
        stmt: str,
        values: Values = None,
    ) -> ExecuteResults:
        return self.execute(
            stmt, values, fetchone=False, fetchall=True, executemany=False
        )

    def fetchone(
        self,
        stmt: str,
        values: Values = None,
    ) -> typing.Optional[list[typing.Any]]:
        return self.execute(
            stmt, values, fetchone=True, fetchall=False, executemany=False
        )

    def _cursor(self):
        """Asserts that the connection is open and returns a cursor"""
        if self._conn is None:
            self._conn = self._pool.getconn()
            self._conn.autocommit = True
        return self._conn.cursor()

    def _execute(self, stmt, *values):
        """
        Gets a cursor, executes `stmt` and closes the cursor,
        fetching one row afterwards and returning its result.
        """
        with self._cursor() as cursor:
            try:
                cursor.execute(stmt, values)
                if cursor.rowcount > 0 and stmt[:6] not in [
                    "insert",
                    "update",
                    "delete",
                ]:
                    return cursor.fetchone()
                else:
                    return None
            except psycopg2.ProgrammingError:
                return None
            finally:
                cursor.close()
                self._pool.putconn(self._conn)
                self._conn = None

    def execute(
        self,
        stmt: str,
        values: Values = None,
        fetchone=True,
        fetchall=False,
        executemany=False,
    ) -> None:
        if all((fetchone, fetchall)):
            raise ValueError("Choose fetchall or fetchone")

        with self._cursor() as cursor:
            try:
                if executemany:
                    cursor.executemany(stmt, values)
                else:
                    cursor.execute(stmt, values)
                results = cursor.rowcount

                if results > 0 and stmt[:6] not in [
                    "insert",
                    "update",
                    "delete",
                ]:
                    if fetchall:
                        return cursor.fetchall()  # type: ignore
                    elif fetchone:
                        return cursor.fetchone()  # type: ignore
                    else:
                        return None
                else:
                    return None
            except psycopg2.ProgrammingError:
                return None
            finally:
                cursor.close()
                self._pool.putconn(self._conn)
                self._conn = None

    def executemany(
        self,
        stmt: str,
        values: Values = None,
    ) -> ExecuteResults:
        return self.execute(
            stmt, values, fetchall=False, fetchone=False, executemany=True
        )

    def close(self) -> None:
        if self._conn is not None:
            self._conn.commit()
            self._conn.close()
            self._conn = None

    def delete(self) -> None:
        raise NotImplementedError

    @classmethod
    def list_sessions(cls: SQLiteSession) -> None:
        raise NotImplementedError

    def _update_session_table(self) -> None:

        self.execute("delete from sessions")

        # data from MemorySession.__init__
        auth_key = (
            psycopg2.Binary(self._auth_key.key) if self._auth_key else b""
        )
        data = {
            "dc_id": self._dc_id if self._dc_id else DEFAULT_DC_ID,
            "server": (
                self._server_address
                if self._server_address
                else DEFAULT_IPV4_IP
            ),
            "port": self._port if self._port else DEFAULT_PORT,
            "auth_key": auth_key,
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

        logger.debug("process_entities", tlo, type(tlo))

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
        logger.debug(f"Row with auth_key = {row}")
        if row and row[0] and row[0][0]:
            self._auth_key = AuthKey(data=row[0][0].tobytes())
        else:
            self._auth_key = None

    def get_update_states(self) -> typing.Any:
        rows = self.fetchall(
            "select id, pts, qts, date, seq from update_state"
        )

        logger.debug("get_update_states", rows)

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
                    file_size bigint,
                    type integer,
                    id bigint,
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

    @staticmethod
    def _get_direct_db_connect(self):
        return psycopg2.connect(
            host=settings.db_host,
            port=settings.db_port,
            dbname=settings.db_name,
            user=settings.db_username,
            password=settings.db_password,
        )

    def _create_schema(self, schema_name):
        with self._get_direct_db_connect(self) as conn:
            with conn.cursor() as cur:
                query = """
                create schema if not exists %s
                """
                cur.execute(query, (AsIs(schema_name),))

    def set_update_state(self, entity_id, state):
        self._execute(
            "insert into update_state values (%s, %s, %s, %s, %s) "
            "on conflict (id) do update set "
            "pts = excluded.pts, "
            "qts = excluded.qts, "
            "date = excluded.date, "
            "seq = excluded.seq;",
            entity_id,
            state.pts,
            state.qts,
            state.date.timestamp(),
            state.seq,
        )
