import yaml
import datetime
import decimal
from textwrap import dedent
import pytest

from datacube.model.fields import get_dataset_fields, parse_search_field, Expression
from datacube.model import Range, metadata_from_doc

METADATA_DOC = yaml.safe_load('''---
name: test
description: test all simple search field types
dataset:
  id: [id]
  sources: [lineage, source_datasets]
  label: [label]
  creation_dt: [creation_dt]
  search_fields:
    x_default_type:
       description: string type is assumed
       offset: [some, path, x_default_type_path]

    x_string:
      type: string
      description: field of type 'string'
      offset: [x_string_path]

    x_double:
      type: double
      description: field of type 'double'
      offset: [x_double_path]

    x_integer:
      type: integer
      description: field of type 'integer'
      offset: [x_integer_path]

    x_numeric:
      type: numeric
      description: field of type 'numeric'
      offset: [x_numeric_path]

    x_datetime:
      type: datetime
      description: field of type 'datetime'
      offset: [x_datetime_path]
''')

SAMPLE_DOC = yaml.safe_load('''---
x_string_path: some_string
x_double_path: 6.283185307179586
x_integer_path: 4466778
x_numeric_path: '100.33'
x_datetime_path: 1999-04-15 12:33:55.001
some:
  path:
    x_default_type_path: just_a_string
''')

METADATA_DOC_RANGES = yaml.safe_load('''---
name: test
description: test all simple search field types
dataset:
  id: [id]
  sources: [lineage, source_datasets]
  label: [label]
  creation_dt: [creation_dt]
  search_fields:
     t_range:
       type: datetime-range
       min_offset: [[t,a], [t,b]]
       max_offset: [[t,a], [t,b]]

     x_range:
       type: double-range
       min_offset: [[x,a], [x,b], [x,c], [x,d]]
       max_offset: [[x,a], [x,b], [x,c], [x,d]]

     float_range:
       type: float-range
       description: float-range is alias for numeric-range
       min_offset: [[a]]
       max_offset: [[b]]

     ab:
       type: integer-range
       min_offset: [[a]]
       max_offset: [[b]]
''')

SAMPLE_DOC_RANGES = yaml.safe_load('''---
t:
  a: 1999-04-15
  b: 1999-04-16
x:
  a: 1
  b: 2
  c: 3
  d: 4
''')


def test_get_dataset_simple_fields():
    xx = get_dataset_fields(METADATA_DOC)
    assert xx['x_default_type'].type_name == 'string'

    type_map = dict(
        double=float,
        integer=int,
        string=str,
        datetime=datetime.datetime,
        numeric=decimal.Decimal,
    )

    for n, f in xx.items():
        assert n == f.name
        assert isinstance(f.description, str)

        expected_type = type_map.get(f.type_name)
        vv = f.extract(SAMPLE_DOC)
        assert isinstance(vv, expected_type)

        # missing data should return None
        assert f.extract({}) is None


def test_get_dataset_range_fields():
    xx = get_dataset_fields(METADATA_DOC_RANGES)
    v = xx['x_range'].extract(SAMPLE_DOC_RANGES)
    assert v == Range(1, 4)

    v = xx['t_range'].extract(SAMPLE_DOC_RANGES)
    assert v.begin.strftime('%Y-%m-%d') == "1999-04-15"
    assert v.end.strftime('%Y-%m-%d') == "1999-04-16"

    # missing range should return None
    assert xx['ab'].extract({}) is None

    # partially missing Range
    assert xx['ab'].extract(dict(a=3)) == Range(3, None)
    assert xx['ab'].extract(dict(b=4)) == Range(None, 4)

    # float-range conversion
    assert xx['float_range'].type_name == 'numeric-range'


def test_metadata_from_doc():
    mm = metadata_from_doc(METADATA_DOC)
    assert mm.definition is METADATA_DOC

    rdr = mm.dataset_reader(SAMPLE_DOC)
    assert rdr.x_double == SAMPLE_DOC['x_double_path']
    assert rdr.x_integer == SAMPLE_DOC['x_integer_path']
    assert rdr.x_string == SAMPLE_DOC['x_string_path']
    assert rdr.x_numeric == decimal.Decimal(SAMPLE_DOC['x_numeric_path'])


def test_bad_field_definition():
    def doc(s):
        return yaml.safe_load(dedent(s))

    with pytest.raises(ValueError):
        parse_search_field(doc('''
        type: bad_type
        offset: [a]
        '''))

    with pytest.raises(ValueError):
        parse_search_field(doc('''
        type: badtype-range
        offset: [a]
        '''))

    with pytest.raises(ValueError):
        parse_search_field(doc('''
        type: double
        description: missing offset
        '''))

    with pytest.raises(ValueError):
        parse_search_field(doc('''
        type: double-range
        description: missing min_offset
        max_offset: [[a]]
        '''))

    with pytest.raises(ValueError):
        parse_search_field(doc('''
        type: double-range
        description: missing max_offset
        min_offset: [[a]]
        '''))


def test_expression():
    assert Expression() == Expression()
    assert (Expression() == object()) is False
