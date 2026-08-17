from collections import defaultdict
from sql_metadata.parser import Parser
import os

from openai import OpenAI


def call_gpt(user_prompt, system_prompt="You are a helpful assistant.", model="model", openai_api_key="EMPTY", openai_api_base="url", max_tokens=12000, temperature=1.0):
    client = OpenAI(
        api_key=openai_api_key,
        base_url=openai_api_base,
    )
    chat_response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        max_tokens=max_tokens,
        n=1,
        temperature=temperature,
        top_p=0.95,
    )
    rsp = chat_response.choices[0].message.content
    return rsp


SQL_KEYWORDS_BLACKLIST = {
    'SUM', 'AVG', 'MAX', 'MIN', 'COUNT', 'CAST', 'COALESCE', 'CONVERT', 'ROUND', 
    'TRIM', 'SUBSTRING', 'REPLACE', 'GETDATE', 'DATE', 'YEAR', 'MONTH', 'DAY',
    'EXTRACT', 'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'LAG', 'LEAD', 'SELECT', 
    'FROM', 'WHERE', 'GROUP', 'BY', 'ORDER', 'HAVING', 'AS', 'ON', 'JOIN', 
    'INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER', 'CASE', 'WHEN', 'THEN',
    'ELSE', 'END', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN', 'IS', 'NULL',
    'LIMIT', 'DISTINCT', 'UNION', 'ALL', 'CREATE', 'TABLE', 'INSERT', 'INTO',
    'VALUES', 'UPDATE', 'SET', 'DELETE', 'INT', 'INTEGER', 'VARCHAR', 'CHAR', 
    'TEXT', 'DATE', 'DATETIME', 'TIMESTAMP', 'REAL', 'FLOAT', 'DOUBLE', 
    'NUMERIC', 'DECIMAL', 'BOOLEAN', 'NAME', 'VALUE', 'KEY', 'TYPE', 'TIME'
}
def get_used_table_and_columns(sql_query: str) -> str:
    """Parse the tables and columns referenced by a SQL query."""
    try:
        parser = Parser(sql_query)
        raw_tables = parser.tables
        
        used_tables = {
            t.lower() for t in raw_tables 
            if t.upper() not in SQL_KEYWORDS_BLACKLIST
        }
        # Ignore '*'.
        used_columns = {col.lower() for col in parser.columns if col != '*'}

    except Exception as e:
        # Fall back on parse failure.
        print(f"Failed to parse SQL '{sql_query}': {e}")
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
            "full": full
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
            table, col = col.split('.')
            table_cols[table].append(col)
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
                col = line[1:].split(":")[0].lower()
                if col in cols:
                    output_str += (f"{line}\n")
                    used_tables.add(table)

    output_str += ("【Foreign keys】\n")
    for line in schema["fks"].split("\n")[1:]:
        try:
            s, t = line.split("=")
            s, t = s.split(".")[0].lower(), t.split(".")[0].lower()
            if s in used_tables or t in used_tables:
                output_str += (f"{line}\n")
        except Exception as e:
            print(e) 
            continue
                    
    return output_str
