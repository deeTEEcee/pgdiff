import datetime

from fabulous.color import bold, blue, red, green

from psycopg2 import connect, sql
from psycopg2.extras import DictCursor

# TODO eh: put this in a config file.
from tabulate import tabulate

# arg options to add:
# * count-only
# * ignore-columns

IGNORE_COLUMNS = {'capturetime', '_id'}


class DBInfo:

    def __init__(self, schema='public', port=None):
        self.connection_args = dict(
            user='postgres',
            password='postgres',
            host='localhost',
            dbname='tenant_storage',
            port=port
        )
        self.schema = schema
        self.connection = connect(
            **self.connection_args
        )

    # def __enter__(self):
    #     self.connection = connect(
    #         **self.connection_args
    #     )
    #     self.cursor = self.connection.cursor()
    #     self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    #     return self
    #
    # def __exit__(self, type, value, traceback):
    #     self.connection.close()

    def get_table_names(self):
        table_names = []
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{self.schema}'")
            for table in cursor.fetchall():
                table_name = table[0]
                table_names.append(table_name)
        return table_names

    def get_column_names(self, table_name):
        with self.connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(f"SELECT * FROM \"{self.schema}\".{table_name} LIMIT 0")
            colnames = [desc[0] for desc in cursor.description if desc[0] not in IGNORE_COLUMNS]
            return colnames

    def get_data(self, table_name, column_names):
        rows = []
        query = sql.SQL("SELECT {fields} FROM {schema_name}.{table_name} ORDER BY endtime ASC").format(
            fields=sql.SQL(',').join(map(sql.Identifier, column_names)),
            schema_name=sql.Identifier(self.schema),
            table_name=sql.Identifier(table_name)
        )

        with self.connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(query)
            for counter, row in enumerate(cursor, start=1):
                rows.append(row)
        return rows


class DiffSummary:

    @classmethod
    def _conv_data(cls, value):
        if type(value) == datetime.datetime:
            return value.isoformat()
        else:
            return str(value)

    @classmethod
    def _diff(cls, row1, row2):
        comp = zip(row1, row2)
        row = []
        has_diff = False
        for x, y in comp:
            if x != y:
                x = cls._conv_data(x)
                y = cls._conv_data(y)
                row.append(f"{bold(red(x))} {bold(blue('->'))} {bold(green(y))}")
                has_diff = True
            else:
                x = cls._conv_data(x)
                row.append(x)
            # TODO: consider showing matches as well
        return row, has_diff

    @classmethod
    def print(cls, table1, table2, table_name, header, count_only=False):
        print(f"Comparing {table_name}")
        mismatch_limit = 30
        mismatch_count = 0
        if len(table1) != len(table2):
            print(f"{table_name} data doesn't match: {len(table1)} != {len(table2)}")
        elif len(table1) == 0 or len(table2) == 0:
            print(f"One of the tables ({table1}, {table2}) is empty.")
        else:
            print(f"Count matches: {len(table1)}")

        if count_only:
            return

        # Table should include diffs and header
        table = [header]
        i = 0

        while i < len(table1):
            row, has_diff = cls._diff(table1[i], table2[i])
            if has_diff:
                table.append(row)
                mismatch_count += 1
            if mismatch_count == mismatch_limit:
                print("Mismatch limit hit.")
                break
            i += 1
        if len(table) > 1:
            print(tabulate(table))


def main():
    db1 = DBInfo(schema='6064a165612089001094637f', port=5434)  # Flink Lives Matter
    db2 = DBInfo(schema='6064ae9d86926e000dce36d7', port=5433)  # Flink Lives Don't Matter
    db1_tables = db1.get_table_names()
    db2_tables = db2.get_table_names()
    if set(db1_tables) != set(db2_tables):
        print(f"Tables don't match: {db1_tables} != {db2_tables}")
        exit(1)

    for table_name in db1_tables:
        db1_data = []
        db2_data = []
        db1_column_names = db1.get_column_names(table_name)
        db2_column_names = db2.get_column_names(table_name)

        if db1_column_names != db2_column_names:
            print("Column names don't match:")
            print(db1_data)
            print(db2_data)
            exit(1)

        column_names = db1_column_names
        db1_data.extend(db1.get_data(table_name, column_names))
        db2_data.extend(db2.get_data(table_name, column_names))
        DiffSummary.print(db1_data, db2_data, table_name, column_names, count_only=False)
        print("")

main()
