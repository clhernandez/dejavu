import queue
from typing import List, Tuple

import psycopg2
from psycopg2.extras import DictCursor, execute_values

from dejavu.base_classes.common_database import CommonDatabase
from dejavu.config.settings import (FIELD_FILE_SHA1, FIELD_FINGERPRINTED,FIELD_SONGTYPE,
                                    FIELD_HASH, FIELD_OFFSET, FIELD_SONG_ID,
                                    FIELD_SONGNAME, FIELD_TOTAL_HASHES,
                                    FINGERPRINTS_TABLENAME, SONGS_TABLENAME, SCHEMA)


class CockroachdbSQLDatabase(CommonDatabase):
    type = "cockroach"

    # CREATES
    CREATE_SONGS_TABLE = f"""
        CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{SONGS_TABLENAME}" (
            "{FIELD_SONG_ID}" uuid DEFAULT uuid_generate_v4()
        ,   "{FIELD_SONGNAME}" VARCHAR(250) NOT NULL
        ,   "{FIELD_FINGERPRINTED}" SMALLINT DEFAULT 0
        ,   "{FIELD_FILE_SHA1}" BYTEA
        ,   "{FIELD_TOTAL_HASHES}" INT4 NOT NULL DEFAULT 0
        ,   "{FIELD_SONGTYPE}" VARCHAR(250) NULL
        ,   "date_created" TIMESTAMP NOT NULL DEFAULT now()
        ,   "date_modified" TIMESTAMP NOT NULL DEFAULT now()
        ,   CONSTRAINT "pk_{SONGS_TABLENAME}_{FIELD_SONG_ID}" PRIMARY KEY ("{FIELD_SONG_ID}")
        ,   CONSTRAINT "uq_{SONGS_TABLENAME}_{FIELD_SONG_ID}" UNIQUE ("{FIELD_SONG_ID}")
        );
    """

    CREATE_FINGERPRINTS_TABLE = f"""
        CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{FINGERPRINTS_TABLENAME}" (
            "{FIELD_HASH}" BYTEA PRIMARY KEY USING HASH
        ,   "{FIELD_SONG_ID}" uuid NOT NULL
        ,   "{FIELD_OFFSET}" INT4 NOT NULL
        ,   "date_created" TIMESTAMP NOT NULL DEFAULT now()
        ,   "date_modified" TIMESTAMP NOT NULL DEFAULT now()
        ,   CONSTRAINT "uq_{FINGERPRINTS_TABLENAME}" UNIQUE  ("{FIELD_SONG_ID}", "{FIELD_OFFSET}", "{FIELD_HASH}")
        ,   CONSTRAINT "fk_{FINGERPRINTS_TABLENAME}_{FIELD_SONG_ID}" FOREIGN KEY ("{FIELD_SONG_ID}")
                REFERENCES "{SCHEMA}"."{SONGS_TABLENAME}"("{FIELD_SONG_ID}") ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS "ix_{FINGERPRINTS_TABLENAME}_{FIELD_HASH}" ON "{SCHEMA}"."{FINGERPRINTS_TABLENAME}"("{FIELD_HASH}") USING hash ;
    """

    CREATE_FINGERPRINTS_TABLE_INDEX = f"""
        CREATE INDEX "ix_{FINGERPRINTS_TABLENAME}_{FIELD_HASH}" ON "{SCHEMA}"."{FINGERPRINTS_TABLENAME}"("{FIELD_HASH}") USING hash ;
    """

    # INSERTS (IGNORES DUPLICATES)
    INSERT_FINGERPRINT = f"""
        INSERT INTO "{SCHEMA}"."{FINGERPRINTS_TABLENAME}" (
                "{FIELD_SONG_ID}"
            ,   "{FIELD_HASH}"
            ,   "{FIELD_OFFSET}")
        VALUES %s ON CONFLICT DO NOTHING;
    """

    INSERT_SONG = f"""
        INSERT INTO "{SCHEMA}"."{SONGS_TABLENAME}" ("{FIELD_SONGNAME}", "{FIELD_FILE_SHA1}","{FIELD_TOTAL_HASHES}","{FIELD_SONGTYPE}")
        VALUES (%s, decode(%s, 'hex'), %s, %s)
        RETURNING "{FIELD_SONG_ID}";
    """

    # SELECTS
    SELECT = f"""
        SELECT "{FIELD_SONG_ID}", "{FIELD_OFFSET}"
        FROM "{SCHEMA}"."{FINGERPRINTS_TABLENAME}"
        WHERE "{FIELD_HASH}" = decode(%s, 'hex');
    """

    SELECT_MULTIPLE = f"""
        SELECT upper(encode("{FIELD_HASH}", 'hex')), "{FIELD_SONG_ID}", "{FIELD_OFFSET}"
        FROM "{SCHEMA}"."{FINGERPRINTS_TABLENAME}"
        WHERE "{FIELD_HASH}" IN (%s);
    """

    SELECT_ALL = f'SELECT "{FIELD_SONG_ID}", "{FIELD_OFFSET}" FROM "{SCHEMA}"."{FINGERPRINTS_TABLENAME}";'

    SELECT_SONG = f"""
        SELECT
            "{FIELD_SONGNAME}"
        ,   upper(encode("{FIELD_FILE_SHA1}", 'hex')) AS "{FIELD_FILE_SHA1}"
        ,   "{FIELD_SONGTYPE}"
        ,   "{FIELD_TOTAL_HASHES}"
        FROM "{SCHEMA}"."{SONGS_TABLENAME}"
        WHERE "{FIELD_SONG_ID}" = %s;
    """

    SELECT_NUM_FINGERPRINTS = f'SELECT COUNT(*) AS n FROM "{SCHEMA}"."{FINGERPRINTS_TABLENAME}";'

    SELECT_UNIQUE_SONG_IDS = f"""
        SELECT COUNT("{FIELD_SONG_ID}") AS n
        FROM "{SCHEMA}"."{SONGS_TABLENAME}"
        WHERE "{FIELD_FINGERPRINTED}" = 1;
    """

    SELECT_SONGS = f"""
        SELECT
            "{FIELD_SONG_ID}"
        ,   "{FIELD_SONGNAME}"
        ,   "{FIELD_SONGTYPE}"
        ,   upper(encode("{FIELD_FILE_SHA1}", 'hex')) AS "{FIELD_FILE_SHA1}"
        ,   "{FIELD_TOTAL_HASHES}"
        ,   "date_created"
        FROM "{SCHEMA}"."{SONGS_TABLENAME}"
        WHERE "{FIELD_FINGERPRINTED}" = 1;
    """

    # DROPS
    DROP_FINGERPRINTS = F'DROP TABLE IF EXISTS "{SCHEMA}"."{FINGERPRINTS_TABLENAME}";'
    DROP_SONGS = F'DROP TABLE IF EXISTS "{SCHEMA}"."{SONGS_TABLENAME}";'

    # UPDATE
    UPDATE_SONG_FINGERPRINTED = f"""
        UPDATE "{SCHEMA}"."{SONGS_TABLENAME}" SET
            "{FIELD_FINGERPRINTED}" = 1
        ,   "date_modified" = now()
        WHERE "{FIELD_SONG_ID}" = %s;
    """

    # DELETES
    DELETE_UNFINGERPRINTED = f"""
        DELETE FROM "{SCHEMA}"."{SONGS_TABLENAME}" WHERE "{FIELD_FINGERPRINTED}" = 0;
    """

    DELETE_SONGS = f"""
        DELETE FROM "{SCHEMA}"."{SONGS_TABLENAME}" WHERE "{FIELD_SONG_ID}" IN (%s);
    """

    # IN
    IN_MATCH = f"decode(%s, 'hex')"

    def __init__(self, **options):
        super().__init__()
        self.cursor = cursor_factory(**options)
        self._options = options

    def after_fork(self) -> None:
        # Clear the cursor cache, we don't want any stale connections from
        # the previous process.
        Cursor.clear_cache()

    def insert_song(self, song_name: str, file_hash: str, total_hashes: int, type: str = None) -> int:
        """
        Inserts a song name into the database, returns the new
        identifier of the song.

        :param song_name: The name of the song.
        :param file_hash: Hash from the fingerprinted file.
        :param total_hashes: amount of hashes to be inserted on fingerprint table.
        :return: the inserted id.
        """
        with self.cursor() as cur:
            cur.execute(self.INSERT_SONG, (song_name, file_hash, total_hashes,type))
            return cur.fetchone()[0]

    def insert_hashes(self, song_id: int, hashes: List[Tuple[str, int]], batch_size: int = 10) -> None:
        """
        Insert a multitude of fingerprints.
        :param song_id: Song identifier the fingerprints belong to
        :param hashes: A sequence of tuples in the format (hash, offset)
            - hash: Part of a sha1 hash, in hexadecimal format
            - offset: Offset this hash was created from/at.
        :param batch_size: insert batches.
        """
        values = [(song_id, hsh, int(offset)) for hsh, offset in hashes]
        # execute_values is way faster than executemany like 60x faster
        # https://stackoverflow.com/questions/8134602/psycopg2-insert-multiple-rows-with-one-query
        with self.cursor() as cur:
            # start = time()
            print("Inserting %s hashes into song_id: %d", len(values), song_id)
            execute_values(cur, self.INSERT_FINGERPRINT, values, template="(%s, decode(%s, 'hex'), %s)")
            # print(f"Inserted {len(values)} hashes in {time() - start} seconds.")

    def __getstate__(self):
        return self._options,

    def __setstate__(self, state):
        self._options, = state
        self.cursor = cursor_factory(**self._options)


def cursor_factory(**factory_options):
    def cursor(**options):
        options.update(factory_options)
        return Cursor(**options)
    return cursor


class Cursor(object):
    """
    Establishes a connection to the database and returns an open cursor.
    # Use as context manager
    with Cursor() as cur:
        cur.execute(query)
        ...
    """
    def __init__(self, dictionary=False, **options):
        super().__init__()

        self._cache = queue.Queue(maxsize=5)

        try:
            conn = self._cache.get_nowait()
            # Ping the connection before using it from the cache.
            conn.ping(True)
        except queue.Empty:
            conn = psycopg2.connect(**options)

        self.conn = conn
        self.dictionary = dictionary

    @classmethod
    def clear_cache(cls):
        cls._cache = queue.Queue(maxsize=5)

    def __enter__(self):
        if self.dictionary:
            self.cursor = self.conn.cursor(cursor_factory=DictCursor)
        else:
            self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, extype, exvalue, traceback):
        # if we had a PostgreSQL related error we try to rollback the cursor.
        if extype is psycopg2.DatabaseError:
            self.cursor.rollback()

        self.cursor.close()
        self.conn.commit()

        # Put it back on the queue
        try:
            self._cache.put_nowait(self.conn)
        except queue.Full:
            self.conn.close()
