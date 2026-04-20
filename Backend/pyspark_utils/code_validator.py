import re


FORBIDDEN_PATTERNS = [
    r"import\s+",
    r"spark\.read",
    r"SparkSession",
    r"\.write\(",
    r"\.save\(",
    r"\.collect\(",
    r"\.count\(\)",
    r"open\(",
    r"subprocess",
    r"eval\(",
]

STRING_COLUMN_FUNCTIONS = (
    "avg",
    "mean",
    "sum",
    "min",
    "max",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "variance",
    "var_pop",
    "var_samp",
    "countDistinct",
    "approx_count_distinct",
    "percentile_approx",
    "first",
    "last",
    "upper",
    "lower",
    "trim",
    "ltrim",
    "rtrim",
    "to_date",
    "to_timestamp",
    "year",
    "month",
    "dayofmonth",
    "datediff",
    "partitionBy",
    "orderBy",
)


def validate_pyspark_code(code: str, available_columns: list[str] | None = None) -> None:
    """
    Enforce strict PySpark execution rules and validate evolving DataFrame schemas.
    """
    for pattern in FORBIDDEN_PATTERNS:
        if re.search(pattern, code):
            raise RuntimeError(
                f"Unsafe PySpark code generated.\n"
                f"Forbidden pattern: `{pattern}`\n\n{code}"
            )

    if "final_df" not in code and "result_df" not in code:
        raise RuntimeError("Generated PySpark code must define `final_df` or `result_df`")

    if "groupBy()" in code:
        raise RuntimeError("Invalid PySpark code: groupBy() must include columns")

    if available_columns:
        _validate_dataframe_lineage(code, available_columns)


def _validate_dataframe_lineage(code: str, available_columns: list[str]) -> None:
    schemas: dict[str, set[str]] = {"df": set(available_columns)}

    for statement in _iter_statements(code):
        if not statement or statement.startswith("#") or "=" not in statement:
            continue

        lhs, rhs = statement.split("=", 1)
        lhs = lhs.strip()
        rhs = rhs.strip()

        if not re.fullmatch(r"[A-Za-z_]\w*", lhs):
            continue

        inferred_schema = _infer_schema(rhs, schemas)
        if inferred_schema is not None:
            schemas[lhs] = inferred_schema

    if schemas.get("final_df") is None and schemas.get("result_df") is None:
        raise RuntimeError("Generated PySpark code must assign a final DataFrame output")


def _iter_statements(code: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    depth = 0

    for raw_line in code.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        if stripped.startswith("#") and not current:
            continue

        current.append(stripped)
        depth += sum(1 for ch in stripped if ch in "([{")
        depth -= sum(1 for ch in stripped if ch in ")]}")

        if depth <= 0:
            statements.append(" ".join(current))
            current = []
            depth = 0

    if current:
        statements.append(" ".join(current))

    return statements


def _infer_schema(expr: str, schemas: dict[str, set[str]]) -> set[str] | None:
    base_var = _extract_base_var(expr)
    if base_var is None:
        return None

    current_schema = set(schemas.get(base_var, set()))
    if not current_schema and base_var != "df":
        return None

    pending_group_cols: list[str] | None = None

    for method_name, args in _extract_method_calls(expr):
        validator_schema = set(current_schema)

        if method_name == "join":
            join_target = _first_non_keyword_arg(args)
            join_schema = _resolve_expr_schema(join_target, schemas)
            validator_schema |= join_schema
            _validate_known_columns(args, validator_schema, method_name)
            current_schema |= join_schema
            continue

        if method_name in {"unionByName", "union", "intersect", "exceptAll"}:
            other_target = _first_non_keyword_arg(args)
            other_schema = _resolve_expr_schema(other_target, schemas)
            validator_schema |= other_schema
            _validate_known_columns(args, validator_schema, method_name)
            if method_name in {"unionByName", "union"}:
                current_schema |= other_schema
            continue

        _validate_known_columns(args, validator_schema, method_name)

        if method_name in {"alias", "filter", "where", "orderBy", "sort", "distinct", "dropDuplicates", "limit"}:
            continue

        if method_name == "withColumn":
            new_col = _first_string_literal(args)
            if new_col:
                current_schema.add(new_col)
            continue

        if method_name == "withColumnRenamed":
            arg_list = _split_top_level_args(args)
            if len(arg_list) >= 2:
                old_name = _string_literal_value(arg_list[0])
                new_name = _string_literal_value(arg_list[1])
                if old_name and new_name and old_name in current_schema:
                    current_schema.remove(old_name)
                    current_schema.add(new_name)
            continue

        if method_name == "drop":
            for arg in _split_top_level_args(args):
                col_name = _string_literal_value(arg)
                if col_name in current_schema:
                    current_schema.remove(col_name)
            continue

        if method_name == "groupBy":
            pending_group_cols = _parse_projection_names(args, current_schema)
            continue

        if method_name == "agg":
            agg_cols = _parse_projection_names(args, current_schema)
            current_schema = set(pending_group_cols or []) | set(agg_cols)
            pending_group_cols = None
            continue

        if method_name == "select":
            current_schema = set(_parse_projection_names(args, current_schema))
            pending_group_cols = None
            continue

        if method_name == "selectExpr":
            current_schema = set(_parse_select_expr_projection_names(args, current_schema))
            pending_group_cols = None
            continue

        if method_name == "toDF":
            renamed_cols = [
                _string_literal_value(arg)
                for arg in _split_top_level_args(args)
                if _string_literal_value(arg)
            ]
            if renamed_cols:
                current_schema = set(renamed_cols)
            continue

    return current_schema


def _validate_known_columns(args: str, schema: set[str], method_name: str) -> None:
    if not schema:
        return

    for col_name in _extract_column_references(args, method_name):
        if col_name not in schema:
            raise RuntimeError(
                f"Column validation failed: `{col_name}` is not available at this step. "
                f"Known columns: {sorted(schema)}"
            )


def _extract_column_references(args: str, method_name: str) -> set[str]:
    refs = set(re.findall(r'(?:F\.)?col\(\s*["\']([^"\']+)["\']\s*\)', args))
    refs |= set(re.findall(r'\[\s*["\']([^"\']+)["\']\s*\]', args))

    function_pattern = "|".join(STRING_COLUMN_FUNCTIONS)
    refs |= set(re.findall(
        rf'(?:F\.|Window\.)?(?:{function_pattern})\(\s*["\']([^"\']+)["\']',
        args
    ))

    if method_name in {"select", "groupBy", "orderBy", "sort", "drop", "dropDuplicates", "partitionBy"}:
        for arg in _split_top_level_args(args):
            value = _string_literal_value(arg)
            if value and value != "*":
                refs.add(value)

    if method_name == "withColumnRenamed":
        arg_list = _split_top_level_args(args)
        if arg_list:
            value = _string_literal_value(arg_list[0])
            if value:
                refs.add(value)

    if method_name == "join":
        on_value = _keyword_arg_value(args, "on")
        if on_value:
            refs |= _extract_join_keys(on_value)
        else:
            positional_args = [part for part in _split_top_level_args(args) if "=" not in part]
            if len(positional_args) >= 2:
                refs |= _extract_join_keys(positional_args[1])

    return refs


def _parse_projection_names(args: str, current_schema: set[str]) -> list[str]:
    names: list[str] = []

    for arg in _split_top_level_args(args):
        alias_name = _extract_alias_name(arg)
        if alias_name:
            names.append(alias_name)
            continue

        string_value = _string_literal_value(arg)
        if string_value:
            if string_value == "*":
                names.extend(sorted(current_schema))
            else:
                names.append(string_value)
            continue

        col_match = re.search(r'(?:F\.)?col\(\s*["\']([^"\']+)["\']\s*\)', arg)
        if col_match:
            names.append(col_match.group(1))

    return names


def _parse_select_expr_projection_names(args: str, current_schema: set[str]) -> list[str]:
    names: list[str] = []

    for arg in _split_top_level_args(args):
        value = _string_literal_value(arg)
        if not value:
            continue

        stripped = value.strip()
        if stripped == "*":
            names.extend(sorted(current_schema))
            continue

        alias_match = re.search(r"(?i)\bas\s+`?([A-Za-z_]\w*)`?\s*$", stripped)
        if alias_match:
            names.append(alias_match.group(1))
            continue

        bare_match = re.fullmatch(r"`?([A-Za-z_]\w*)`?", stripped)
        if bare_match:
            names.append(bare_match.group(1))

    return names


def _extract_alias_name(expr: str) -> str | None:
    match = re.search(r'\.alias\(\s*["\']([^"\']+)["\']\s*\)\s*$', expr)
    if match:
        return match.group(1)
    return None


def _extract_base_var(expr: str) -> str | None:
    match = re.match(r'\s*([A-Za-z_]\w*)', expr)
    if match:
        return match.group(1)
    return None


def _resolve_expr_schema(expr: str | None, schemas: dict[str, set[str]]) -> set[str]:
    if not expr:
        return set()

    inferred = _infer_schema(expr, schemas)
    if inferred is not None:
        return inferred

    base_var = _extract_base_var(expr)
    if base_var:
        return set(schemas.get(base_var, set()))

    return set()


def _extract_method_calls(expr: str) -> list[tuple[str, str]]:
    calls: list[tuple[str, str]] = []
    i = 0
    in_single = False
    in_double = False

    while i < len(expr):
        char = expr[i]
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double
        elif char == "." and not in_single and not in_double:
            method_match = re.match(r'\.([A-Za-z_]\w*)\(', expr[i:])
            if method_match:
                method_name = method_match.group(1)
                open_paren = i + len(method_name) + 1
                close_paren = _find_matching_paren(expr, open_paren)
                calls.append((method_name, expr[open_paren + 1:close_paren]))
                i = close_paren
        i += 1

    return calls


def _find_matching_paren(text: str, open_paren: int) -> int:
    depth = 0
    in_single = False
    in_double = False

    for i in range(open_paren, len(text)):
        char = text[i]
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double
        elif not in_single and not in_double:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0:
                    return i

    raise RuntimeError("Generated PySpark code has unbalanced parentheses")


def _split_top_level_args(text: str) -> list[str]:
    parts: list[str] = []
    start = 0
    depth = 0
    in_single = False
    in_double = False

    for i, char in enumerate(text):
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double
        elif not in_single and not in_double:
            if char in "([{":
                depth += 1
            elif char in ")]}":
                depth -= 1
            elif char == "," and depth == 0:
                parts.append(text[start:i].strip())
                start = i + 1

    tail = text[start:].strip()
    if tail:
        parts.append(tail)

    return parts


def _keyword_arg_value(text: str, key: str) -> str | None:
    prefix = f"{key}="
    for part in _split_top_level_args(text):
        stripped = part.strip()
        if stripped.startswith(prefix):
            return stripped[len(prefix):].strip()
    return None


def _first_non_keyword_arg(text: str) -> str | None:
    for part in _split_top_level_args(text):
        stripped = part.strip()
        if "=" not in stripped:
            return stripped
    return None


def _first_string_literal(text: str) -> str | None:
    for arg in _split_top_level_args(text):
        value = _string_literal_value(arg)
        if value is not None:
            return value
    return None


def _string_literal_value(text: str) -> str | None:
    stripped = text.strip()
    if len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in {"'", '"'}:
        return stripped[1:-1]
    return None


def _extract_join_keys(expr: str) -> set[str]:
    refs: set[str] = set()

    literal = _string_literal_value(expr)
    if literal:
        refs.add(literal)
        return refs

    if expr.strip().startswith("[") and expr.strip().endswith("]"):
        refs |= set(re.findall(r'["\']([^"\']+)["\']', expr))

    refs |= set(re.findall(r'(?:F\.)?col\(\s*["\']([^"\']+)["\']\s*\)', expr))
    refs |= set(re.findall(r'\[\s*["\']([^"\']+)["\']\s*\]', expr))
    return refs
