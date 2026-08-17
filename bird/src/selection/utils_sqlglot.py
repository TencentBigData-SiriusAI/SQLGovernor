from collections import defaultdict
import os

from sqlglot import exp, parse_one


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
    "DATE",
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


def _warn_parse_failure(sql_query: str, exc: Exception) -> None:
    head = sql_query.strip().splitlines()[0] if sql_query.strip() else ""
    print(f"Failed to parse SQL backend=sqlglot head={head!r}: {exc}")


def _collect_cte_names(tree):
    names = set()
    for cte in tree.find_all(exp.CTE):
        name = (cte.alias_or_name or "").lower()
        if name:
            names.add(name)
    return names


def _collect_base_tables_and_aliases(tree, cte_names):
    base_tables = set()
    alias_to_table = {}

    for table in tree.find_all(exp.Table):
        table_name = (table.name or "").lower()
        if not table_name:
            continue
        if table_name in cte_names or table_name.upper() in SQL_KEYWORDS_BLACKLIST:
            continue

        base_tables.add(table_name)
        alias_to_table[table_name] = table_name

        alias = (table.alias or "").lower()
        if alias and alias != table_name:
            alias_to_table[alias] = table_name

    return base_tables, alias_to_table


def get_used_table_and_columns(sql_query: str):
    try:
        tree = parse_one(sql_query, read="sqlite")
        cte_names = _collect_cte_names(tree)
        used_tables, alias_to_table = _collect_base_tables_and_aliases(tree, cte_names)
        used_columns = set()

        for column in tree.find_all(exp.Column):
            column_name = (column.name or "").lower()
            if not column_name or column_name == "*":
                continue

            table_name = (column.table or "").lower()
            if table_name:
                resolved_table = alias_to_table.get(table_name, table_name)
                if resolved_table in cte_names:
                    used_columns.add(column_name)
                elif resolved_table.upper() not in SQL_KEYWORDS_BLACKLIST:
                    used_columns.add(f"{resolved_table}.{column_name}")
            else:
                used_columns.add(column_name)
    except Exception as exc:
        _warn_parse_failure(sql_query, exc)
        return [], []

    return used_tables, used_columns


def load_schemas(db_dir):
    schemas = {}
    for db_id in os.listdir(db_dir):
        if "." in db_id:
            continue
        table_schema = {}
        lines = []
        table_name = None
        path = f"{db_dir}/{db_id}/{db_id}.xmschema"

        with open(path) as f:
            f.readline()
            f.readline()
            for line in f:
                if line.startswith("# Table: "):
                    if len(lines) > 0:
                        table_schema[table_name] = lines
                        lines = []
                    table_name = line.split(": ")[1].strip().lower()
                elif line.startswith("【Foreign keys】"):
                    if len(lines) > 0:
                        table_schema[table_name] = lines
                        lines = []
                    lines = [line.strip()]
                elif line.strip() not in ["[", "]"]:
                    lines.append(line.strip())
        with open(path) as f:
            full = f.read()

        schemas[db_id] = {
            "tables": table_schema,
            "fks": "\n".join(lines),
            "full": full,
        }
    return schemas


schemas = load_schemas(os.getenv("DATABASE_DIR", ""))


def get_related_schema(db_id, sqls):
    cols = []
    tables_all = []
    for sql in sqls:
        tables, columns = get_used_table_and_columns(sql)
        if len(columns) == 0:
            return schemas[db_id]["full"]
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
            table, column = col.split(".", 1)
            table_cols[table].append(column)

    schema = schemas[db_id]
    if len(table_cols) == 0:
        return schemas[db_id]["full"]

    output_str = ""
    used_tables = set(table_cols.keys())
    for table in schema["tables"]:
        table = table.lower()
        if table not in table_cols:
            continue
        cols = table_cols.get(table, [])
        if "*" in cols:
            output_str += f"# Table: {table}\n" + "\n".join(schema["tables"][table]) + "\n"
            used_tables.add(table)
        else:
            output_str += f"# Table: {table}\n"
            for line in schema["tables"][table]:
                column_name = line[1:].split(":")[0].lower()
                if column_name in cols:
                    output_str += f"{line}\n"
                    used_tables.add(table)

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


__all__ = ["get_related_schema", "get_used_table_and_columns"]
