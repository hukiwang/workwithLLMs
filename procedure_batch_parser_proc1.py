#!/usr/bin/env python3
"""Convert SAP HANA procedure scripts for Huawei DWS.

This script processes `.sql` files in a given directory, applies a series of
transformations and outputs converted versions alongside aggregated files for
further usage. It mirrors the behaviour of ``ProcedureBatchParserProc1.java``.
"""

from __future__ import annotations

import re
from collections import OrderedDict
from pathlib import Path
from typing import List, Set


def normalize_data_types(sql: str) -> str:
    """Replace NVARCHAR with VARCHAR ignoring case."""
    if not sql:
        return sql
    return re.sub(r"(?i)\bNVARCHAR\b", "VARCHAR", sql)


def convert_date_functions(sql: str) -> str:
    """Convert TO_DATS functions and protect existing TO_CHAR expressions."""
    if not sql:
        return sql

    sql = re.sub(r"(?i)TO_DATS\s*\(([^)]+)\)", r"TO_CHAR(\1,'YYYYMMDD')", sql)

    char_placeholders: OrderedDict[str, str] = OrderedDict()

    def _replace(match: re.Match) -> str:
        placeholder = f"##__TO_CHAR_{len(char_placeholders)}__##"
        char_placeholders[placeholder] = match.group(0)
        return placeholder

    sql = re.sub(r"(?i)TO_CHAR\s*\(([^()]*?)\s*,\s*'YYYYMMDD'\s*\)", _replace, sql)

    for placeholder, original in char_placeholders.items():
        sql = sql.replace(placeholder, original)

    return sql


def transform_temp_table_to_create_table(final_sql: str, temp_table: str, proc_table: str) -> str:
    if not final_sql or not temp_table or not proc_table:
        return final_sql

    new_table_name = f"{proc_table}_MID_TMP"
    pattern = re.compile(rf"(?i)\b{re.escape(temp_table)}\b\s*=\s*SELECT.*?;", re.DOTALL)
    match = pattern.search(final_sql)

    if match:
        matched_block = match.group(0)
        equal_idx = matched_block.find('=')
        if equal_idx != -1:
            select_sql = matched_block[equal_idx + 1:].strip()
            if select_sql.endswith(';'):
                select_sql = select_sql[:-1].strip()
            replacement = (
                f"DROP TABLE IF EXISTS {new_table_name};\n"
                f"CREATE TABLE {new_table_name} WITH(orientation=column) AS \n"
                f"{select_sql}\n;"
            )
            final_sql = final_sql[: match.start()] + replacement + final_sql[match.end() :]

    processed_sql = re.sub(rf"(?i)(?<!:)\b{re.escape(temp_table)}\b", new_table_name, final_sql)

    lines = processed_sql.splitlines()
    final_lines: List[str] = []
    inserted = False
    for line in lines:
        if (
            not inserted
            and "执行成功日志" in line.upper()
            and "BEGIN" in line.upper()
        ):
            inserted = True
        final_lines.append(line)
    return "\n".join(final_lines)


def replace_exec_and_while_syntax(sql: str) -> str:
    if not sql:
        return sql
    lines = sql.splitlines()
    result: List[str] = []

    exec_pattern = re.compile(r"(?i)\bexec\s*:")
    end_while_pattern = re.compile(r"(?i)\bend\s+while\s*;")
    call_sleep_pattern = re.compile(r"(?i)CALL\s+(\S+):SLEEP_SECONDS\((.*?)\);")
    using_sync_pattern = re.compile(
        r"(?i)^\s*USING\s+SQLSCRIPT_SYNC\s+AS\s+SYNCLIB\s*;\s*$"
    )

    for line in lines:
        modified = line
        modified = exec_pattern.sub("execute :", modified)
        if "while :" in modified.lower() and modified.strip().lower().endswith("do"):
            modified = re.sub(r"(?i)\bdo\s*$", "LOOP", modified)
        modified = end_while_pattern.sub("end loop;", modified)
        m = call_sleep_pattern.search(modified)
        if m:
            arg = m.group(2).strip()
            modified = f"PERFORM PG_SLEEP({arg});"
        if using_sync_pattern.search(modified):
            modified = "--" + modified.strip()
        result.append(modified)
    return "\n".join(result)


def remove_colon_before_temp_table(sql: str, temp_table: str) -> str:
    if not sql or not temp_table.strip():
        return sql
    pattern = rf"(?i):\s*{re.escape(temp_table)}\b"
    return re.sub(pattern, temp_table, sql)


def insert_log_block_before_final_end(final_sql: str) -> str:
    if not final_sql:
        return final_sql

    log_block = (
        "\n/*********执行成功日志******BEGIN*********/\n"
        "    CALL ETL_CTRL.PROC_LI_ETL_LOG(\n"
        "        P_INPUT_JOB_SESSION, V_WF_NAME, V_BELONG_LEVEL,\n"
        "        V_WF_STATUS, P_INPUT_DATADATE, '', ''\n"
        "    );\n"
        "    P_OUT_WF_STATUS := V_WF_STATUS;\n"
        "/*********执行成功日志******END***********/\n\n"
        "/*********执行失败日志******BEGIN*********/\n"
        "EXCEPTION \n"
        "    WHEN OTHERS THEN \n"
        "        V_WF_STATUS := 'E';\n"
        "        P_OUT_WF_STATUS := V_WF_STATUS;\n"
        "        CALL ETL_CTRL.PROC_LI_ETL_LOG(\n"
        "            P_INPUT_JOB_SESSION, V_WF_NAME, V_BELONG_LEVEL,\n"
        "            V_WF_STATUS, P_INPUT_DATADATE, SQLSTATE, LEFT(SQLERRM,2000) \n"
        "        );\n"
        "/*********执行失败日志******END***************/\n"
    )

    end_pattern = re.compile(r"(?i)end\s*;", re.DOTALL)
    last_match = None
    for m in end_pattern.finditer(final_sql):
        last_match = m

    if last_match is None:
        return final_sql.strip() + "\n\n" + log_block

    before_end = final_sql[: last_match.start()].strip()
    end_statement = final_sql[last_match.start() : last_match.end()].strip()
    after_end = final_sql[last_match.end() :].strip()

    result = before_end + "\n\n" + log_block + "\n" + end_statement
    if after_end:
        result += "\n" + after_end
    return result


def clean_quotes_and_ensure_final_end(transformed: str) -> str:
    if not transformed:
        return transformed
    no_quotes = transformed.replace('"', '')
    end_pattern = re.compile(r"(?i)END\s*;\s*$", re.DOTALL)
    m = end_pattern.search(no_quotes)
    if m:
        removed_end_sql = no_quotes[: m.start()].strip()
    else:
        removed_end_sql = no_quotes

    lines = removed_end_sql.splitlines()
    has_end = False
    for line in reversed(lines):
        line = line.strip()
        if not line or line.startswith("--") or line.startswith("/*"):
            continue
        if re.match(r"(?i)^end\s*;\s*$", line):
            has_end = True
        break
    if not has_end:
        removed_end_sql += "\nend;"
    return removed_end_sql.strip()


def remove_log_block(sql: str, keyword: str) -> str:
    lines = sql.splitlines()
    result: List[str] = []
    skip = False
    matched = False

    for line in lines:
        clean_line = re.sub(r"\s+", "", line)
        if not skip and "/*" in clean_line and keyword in clean_line and "BEGIN" in clean_line.upper():
            skip = True
            matched = True
            continue
        if skip and "/*" in clean_line and keyword in clean_line and "END" in clean_line.upper():
            skip = False
            continue
        if not skip:
            result.append(line)
    return "\n".join(result) if matched else sql


def remove_exception_block_and_log(sql: str) -> str:
    handler_pattern = re.compile(
        r"(?i)DECLARE\s+EXIT\s+HANDLER\s+FOR\s+SQLEXCEPTION.*?END\s*;",
        re.DOTALL,
    )
    match = handler_pattern.search(sql)
    result = sql
    if match:
        result = sql[: match.start()] + sql[match.end() :]
        lines = result.splitlines()
        builder: List[str] = []
        for line in lines:
            clean = re.sub(r"\s+", "", line)
            if (
                ("执行失败日志" in clean and "BEGIN" in clean.upper())
                or ("执行失败日志" in clean and "END" in clean.upper())
            ):
                continue
            builder.append(line)
        return "\n".join(builder)
    return sql


def transform_procedure_sql(sql: str) -> str:
    try:
        proc_pattern = re.compile(r'(?i)CREATE\s+PROCEDURE\s+"([^"]+)"\."([^"]+)"\s*\(')
        proc_match = proc_pattern.search(sql)
        if not proc_match:
            return sql
        schema = proc_match.group(1)
        procedure = proc_match.group(2)
        param_start = proc_match.end() - 1
        paren_count = 0
        param_end = -1
        for i in range(param_start, len(sql)):
            c = sql[i]
            if c == '(': 
                paren_count += 1
            elif c == ')':
                paren_count -= 1
                if paren_count == 0:
                    param_end = i
                    break
        if param_end == -1:
            return sql
        param_group = re.sub(r"(?i)\bOUT\b", "INOUT", sql[param_start + 1 : param_end]).strip()
        new_head = (
            f"drop PROCEDURE if exists \"{schema}\".\"{procedure}\";\n"
            f"create or replace PROCEDURE \"{schema}\".\"{procedure}\"({param_group})\n AS\n"
        )
        sql = sql[: proc_match.start()] + new_head + sql[param_end + 1 :]
        sql = re.sub(
            r"(?is)LANGUAGE\s+SQLSCRIPT\s*\n\s*SQL\s+SECURITY\s+INVOKER\s*\n\s*DEFAULT\s+SCHEMA\s+SRC.*?\n",
            "",
            sql,
        )
        sql = re.sub(r"(?is)\bAS\b\s*\n\s*BEGIN", "", sql, count=1)
        sql = remove_exception_block_and_log(sql)
        sql = remove_log_block(sql, "执行成功日志")
        return sql.strip()
    except Exception as e:
        print("transformProcedureSQL error:", e)
        return sql


def extract_procedure_table(sql: str) -> str:
    try:
        m = re.search(r'(?i)CREATE\s+PROCEDURE\s+"([^"]+)"\."([^"]+)"', sql)
        if m:
            return f"{m.group(1)}.{m.group(2)}"
    except Exception as e:
        print("extractProcedureTable error:", e)
    return ""


def extract_procedure_params(sql: str) -> str:
    try:
        start_index = sql.upper().find("CREATE PROCEDURE")
        if start_index == -1:
            return ""
        first_paren_index = sql.find("(", start_index)
        if first_paren_index == -1:
            return ""
        paren_count = 0
        end_paren_index = -1
        for i in range(first_paren_index, len(sql)):
            c = sql[i]
            if c == '(': 
                paren_count += 1
            elif c == ')':
                paren_count -= 1
                if paren_count == 0:
                    end_paren_index = i
                    break
        if end_paren_index != -1:
            param_group = sql[first_paren_index + 1 : end_paren_index]
            return re.sub(r"(?i)\bOUT\b", "INOUT", param_group).strip()
    except Exception as e:
        print("extractProcedureParams error:", e)
    return ""


def extract_temp_table_name(sql: str) -> str:
    try:
        m = re.search(r"\b(\w+)\s*=\s*SELECT", sql, re.IGNORECASE)
        if m:
            return m.group(1)
    except Exception as e:
        print("extractTempTableName error:", e)
    return ""


def process_sql_file(
    input_path: Path,
    output_dir: Path,
    all_sql_builder: List[str],
    proc_tables: List[str],
    proc_table_set: Set[str],
) -> None:
    try:
        sql = input_path.read_text(encoding="utf-8")
        proc_table = extract_procedure_table(sql)
        if proc_table and proc_table not in proc_table_set:
            proc_table_set.add(proc_table)
            proc_tables.append(proc_table)
        temp_table = extract_temp_table_name(sql)

        transformed = transform_procedure_sql(sql)
        final_sql = clean_quotes_and_ensure_final_end(transformed)
        completed_sql = insert_log_block_before_final_end(final_sql)

        if temp_table:
            completed_sql = remove_colon_before_temp_table(completed_sql, temp_table)
            completed_sql = transform_temp_table_to_create_table(completed_sql, temp_table, proc_table)
            completed_sql = replace_exec_and_while_syntax(completed_sql)
            completed_sql = convert_date_functions(completed_sql)
            completed_sql = normalize_data_types(completed_sql)
        else:
            completed_sql = replace_exec_and_while_syntax(completed_sql)
            completed_sql = convert_date_functions(completed_sql)
            completed_sql = normalize_data_types(completed_sql)

        output_file = output_dir / input_path.name
        output_file.write_text(completed_sql + "\n/", encoding="utf-8")

        all_sql_builder.append("-- ------------------------------\n")
        all_sql_builder.append(f"-- File: {input_path.name}\n")
        all_sql_builder.append("-- ------------------------------\n")
        all_sql_builder.append(completed_sql + "\n/\n\n")
    except Exception as e:
        print(f"Failed to process {input_path}: {e}")


def main(folder_path: str | None = None) -> None:
    folder = Path(folder_path) if folder_path else Path(r"D:\\002Data\\002RenBao\\002脚本翻译\\005SRC层\\Input")
    input_dir = folder
    output_dir = input_dir.parent / "output"
    output_all_dir = input_dir.parent / "outputAll"
    drop_proc_dir = input_dir.parent / "outputDropProcAll"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_all_dir.mkdir(parents=True, exist_ok=True)
    drop_proc_dir.mkdir(parents=True, exist_ok=True)

    all_sql_builder: List[str] = []
    proc_tables: List[str] = []
    proc_table_set: Set[str] = set()

    for path in input_dir.rglob("*.sql"):
        process_sql_file(path, output_dir, all_sql_builder, proc_tables, proc_table_set)

    (output_all_dir / "all.sql").write_text("".join(all_sql_builder), encoding="utf-8")

    drop_lines = [f"DROP PROCEDURE IF EXISTS {p};\n" for p in proc_tables]
    (drop_proc_dir / "dropProc.sql").write_text("".join(drop_lines), encoding="utf-8")


if __name__ == "__main__":
    import sys

    main(sys.argv[1] if len(sys.argv) > 1 else None)

