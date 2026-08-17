from collections import defaultdict
from sql_metadata.parser import Parser

SQL_KEYWORDS_BLACKLIST = {
    "SUM",
    "AVG",
    "MAX",
    "MIN",
    "COUNT",
    "CAST",
    "COALESCE",
    "CONVERT",
    "ROUND",
    "TRIM",
    "SUBSTRING",
    "REPLACE",
    "GETDATE",
    "DATE",
    "YEAR",
    "MONTH",
    "DAY",
    "EXTRACT",
    "ROW_NUMBER",
    "RANK",
    "DENSE_RANK",
    "LAG",
    "LEAD",
    "SELECT",
    "FROM",
    "WHERE",
    "GROUP",
    "BY",
    "ORDER",
    "HAVING",
    "AS",
    "ON",
    "JOIN",
    "INNER",
    "LEFT",
    "RIGHT",
    "FULL",
    "OUTER",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "AND",
    "OR",
    "NOT",
    "IN",
    "LIKE",
    "BETWEEN",
    "IS",
    "NULL",
    "LIMIT",
    "DISTINCT",
    "UNION",
    "ALL",
    "CREATE",
    "TABLE",
    "INSERT",
    "INTO",
    "VALUES",
    "UPDATE",
    "SET",
    "DELETE",
    "INT",
    "INTEGER",
    "VARCHAR",
    "CHAR",
    "TEXT",
    "DATETIME",
    "TIMESTAMP",
    "REAL",
    "FLOAT",
    "DOUBLE",
    "NUMERIC",
    "DECIMAL",
    "BOOLEAN",
    "NAME",
    "VALUE",
    "KEY",
    "TYPE",
    "TIME",
}


def get_used_table_and_columns(sql_query: str):
    try:
        parser = Parser(sql_query)
        raw_tables = parser.tables
        used_tables = {
            table.lower()
            for table in raw_tables
            if table.upper() not in SQL_KEYWORDS_BLACKLIST
        }
        used_columns = {col.lower() for col in parser.columns if col != "*"}
    except Exception:
        return [], []
    return used_tables, used_columns


def get_related_schema(sqls, schema=None, logger=None):
    cols = []
    tables_all = []
    for sql in sqls:
        tables, columns = get_used_table_and_columns(sql)
        if len(columns) == 0:
            return schema["full"]
        tables_all.extend(tables)
        cols.extend(columns)

    tables_all = set(tables_all)
    cols_all = list(set(cols))
    table_cols = defaultdict(list)
    for col in cols_all:
        if "." not in col:
            for table in tables_all:
                table_cols[table].append(col)
        else:
            table, column = col.split(".")
            table_cols[table].append(column)

    if len(table_cols) == 0:
        if logger is not None:
            logger.warning("no columns found in sqls")
        return schema["full"]

    output_str = ""
    used_tables = set(table_cols.keys())
    for table in schema["tables"]:
        lower_table = table.lower()
        if lower_table not in table_cols:
            continue
        cols = table_cols.get(lower_table, [])
        if "*" in cols:
            output_str += f"# Table: {lower_table}\n" + "\n".join(schema["tables"][table]) + "\n"
            used_tables.add(lower_table)
        else:
            output_str += f"# Table: {lower_table}\n"
            for line in schema["tables"][table]:
                column_name = line[1:].split(":")[0].lower()
                if column_name in cols:
                    output_str += f"{line}\n"
                    used_tables.add(lower_table)

    output_str += "【Foreign keys】\n"
    for line in schema["fks"].split("\n")[1:]:
        try:
            src, dst = line.split("=")
            src_table = src.split(".")[0].lower()
            dst_table = dst.split(".")[0].lower()
            if src_table in used_tables or dst_table in used_tables:
                output_str += f"{line}\n"
        except Exception:
            continue

    return output_str
