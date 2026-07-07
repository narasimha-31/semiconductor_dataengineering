import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from chatbot.supply_chain_bot import validate_sql, clean_sql, ALLOWED_TABLES

GOOD_TABLE = sorted(ALLOWED_TABLES)[0]


def test_select_on_allowed_table_passes():
    ok, result = validate_sql(f"SELECT * FROM `{GOOD_TABLE}` LIMIT 10")
    assert ok
    assert 'LIMIT' in result.upper()


def test_missing_limit_gets_bolted_on():
    ok, result = validate_sql(f"SELECT hs_code FROM `{GOOD_TABLE}`")
    assert ok
    assert result.strip().upper().endswith('LIMIT 100')


def test_delete_blocked():
    ok, reason = validate_sql(f"DELETE FROM `{GOOD_TABLE}` WHERE 1=1")
    assert not ok


def test_drop_blocked():
    ok, reason = validate_sql("DROP TABLE `anything.at.all`")
    assert not ok


def test_mutation_hidden_in_select_blocked():
    ok, reason = validate_sql(
        f"SELECT * FROM `{GOOD_TABLE}`; DELETE FROM `{GOOD_TABLE}`")
    assert not ok
    assert 'DELETE' in reason


def test_unlisted_table_blocked():
    ok, reason = validate_sql(
        "SELECT * FROM `some-other-project.dataset.users` LIMIT 5")
    assert not ok
    assert 'not allowlisted' in reason


def test_no_table_reference_blocked():
    ok, reason = validate_sql("SELECT 1")
    assert not ok


def test_clean_sql_strips_fences():
    fenced = "```sql\nSELECT 1 FROM `x`\n```"
    assert clean_sql(fenced) == "SELECT 1 FROM `x`"