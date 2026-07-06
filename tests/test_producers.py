import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from producers.census_trade_producer import row_to_message, MESSAGE_SCHEMA
from jsonschema import validate, ValidationError
import pytest


HEADER = ['CTY_CODE', 'CTY_NAME', 'GEN_VAL_MO', 'COMM_LVL', 'I_COMMODITY', 'time']


def test_row_to_message_maps_fields():
    row = ['5830', 'TAIWAN', '84355584', 'HS6', '854231', '2010-01']
    msg = row_to_message(HEADER, row)
    assert msg['cty_code'] == '5830'
    assert msg['cty_name'] == 'TAIWAN'
    assert msg['trade_value_usd'] == '84355584'
    assert msg['hs_code'] == '854231'
    assert msg['month'] == '2010-01'


def test_valid_message_passes_schema():
    msg = {
        'cty_code': '5830', 'cty_name': 'TAIWAN', 'hs_code': '854231',
        'trade_value_usd': '84355584', 'month': '2010-01'
    }
    validate(instance=msg, schema=MESSAGE_SCHEMA)


def test_non_numeric_value_fails_schema():
    msg = {
        'cty_code': '5830', 'cty_name': 'TAIWAN', 'hs_code': '854231',
        'trade_value_usd': 'CORRUPTED', 'month': '2010-01'
    }
    with pytest.raises(ValidationError):
        validate(instance=msg, schema=MESSAGE_SCHEMA)


def test_wrong_hs_code_fails_schema():
    msg = {
        'cty_code': '5830', 'cty_name': 'TAIWAN', 'hs_code': '999999',
        'trade_value_usd': '100', 'month': '2010-01'
    }
    with pytest.raises(ValidationError):
        validate(instance=msg, schema=MESSAGE_SCHEMA)


def test_missing_required_field_fails_schema():
    msg = {'cty_code': '5830', 'cty_name': 'TAIWAN'}
    with pytest.raises(ValidationError):
        validate(instance=msg, schema=MESSAGE_SCHEMA)
