"""
Pure-Python implementation of the CSV dialect and timestamp rules (rules.md).

The production pipeline does the same work in DuckDB SQL (sql_dialect.py); this
module is the slow, obviously-correct counterpart. It is used by tests/oracle.py
(an independent reference pipeline) and by tests/test_dialect.py, which checks
that the SQL and the Python versions agree.
"""
import re
from datetime import date

ASCII_DIGITS = re.compile(r"[0-9]+")
F1 = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})Z")
F2 = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})([+-])([0-9]{2}):([0-9]{2})")
_EPOCH_ORDINAL = date(1970, 1, 1).toordinal()


def parse_csv_line(line, expected_fields):
    """Fields of one line (no line ending), or None if the line is malformed. Empty field -> None."""
    fields = line.split(",")
    if len(fields) != expected_fields:
        return None
    result = []
    for field in fields:
        if '"' in field:
            if len(field) < 2 or field[0] != '"' or field[-1] != '"' or '"' in field[1:-1]:
                return None
            field = field[1:-1]
        result.append(None if field == "" else field)
    return result


def read_csv(path, expected_header):
    """
    Yield the parsed data rows of one CSV file. Only "\\n" ends a line ("\\r" is an
    ordinary character) and the first line is the header, whatever it contains.
    """
    with open(path, "rb") as file:
        text = file.read().decode("utf-8")
    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    for line in lines[1:]:
        row = parse_csv_line(line, len(expected_header))
        if row is not None:
            yield row


def parse_timestamp(value):
    """Epoch milliseconds (UTC) for forms F1, F2, F3, or None for a bad timestamp."""
    if value is None:
        return None
    if ASCII_DIGITS.fullmatch(value):
        n = int(value)
        return n if n < 2 ** 63 else None
    offset = 0
    match = F1.fullmatch(value)
    if match:
        year, month, day, hour, minute, second = map(int, match.groups())
    else:
        match = F2.fullmatch(value)
        if not match:
            return None
        year, month, day, hour, minute, second = map(int, match.groups()[:6])
        sign, oh, om = match.group(7), int(match.group(8)), int(match.group(9))
        if oh > 23 or om > 59:
            return None
        offset = (oh * 60 + om) * (-1 if sign == "-" else 1)
    if hour > 23 or minute > 59 or second > 59:
        return None
    try:
        days = date(year, month, day).toordinal() - _EPOCH_ORDINAL   # ValueError: impossible calendar date
    except ValueError:
        return None
    return (days * 86400 + hour * 3600 + minute * 60 + second - offset * 60) * 1000
