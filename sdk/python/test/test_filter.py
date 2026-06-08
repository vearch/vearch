import pytest
from vearch.filter import (
    RelationOperator,
    BooleanOperator,
    BooleanOperators,
    FieldValue,
    Condition,
    Filter,
)


class TestRelationOperator:
    def test_relation_operators_exist(self):
        assert RelationOperator.IN == "IN"
        assert RelationOperator.NOT_IN == "NOT IN"
        assert RelationOperator.EQ == "="
        assert RelationOperator.NE == "!="
        assert RelationOperator.GT == ">"
        assert RelationOperator.GE == ">="
        assert RelationOperator.LT == "<"
        assert RelationOperator.LE == "<="

    def test_relation_operators_are_strings(self):
        for attr in [RelationOperator.IN, RelationOperator.NOT_IN,
                     RelationOperator.GT, RelationOperator.GE,
                     RelationOperator.LT, RelationOperator.LE]:
            assert isinstance(attr, str)


class TestBooleanOperator:
    def test_boolean_operators_exist(self):
        assert BooleanOperator.AND == "AND"
        assert BooleanOperator.OR == "OR"

    def test_boolean_operators_are_strings(self):
        assert isinstance(BooleanOperator.AND, str)
        assert isinstance(BooleanOperator.OR, str)


class TestBooleanOperators:
    def test_boolean_operators_list(self):
        assert isinstance(BooleanOperators, list)
        assert "AND" in BooleanOperators
        assert "OR" in BooleanOperators
        assert len(BooleanOperators) == 2


class TestFieldValue:
    def test_field_value_init(self):
        fv = FieldValue("age", 25)
        assert fv.field == "age"
        assert fv.value == 25

    def test_field_value_with_float(self):
        fv = FieldValue("price", 19.99)
        assert fv.field == "price"
        assert fv.value == 19.99

    def test_field_value_with_list(self):
        fv = FieldValue("country", ["USA", "China", "Japan"])
        assert fv.field == "country"
        assert fv.value == ["USA", "China", "Japan"]

    def test_field_value_with_string(self):
        fv = FieldValue("name", "Alice")
        assert fv.field == "name"
        assert fv.value == "Alice"


class TestCondition:
    def test_condition_init(self):
        fv = FieldValue("age", 30)
        cond = Condition(RelationOperator.GT, fv)
        assert cond.fv == fv
        assert cond.relation_operator == ">"

    def test_condition_dict(self):
        fv = FieldValue("age", 30)
        cond = Condition(RelationOperator.GT, fv)
        d = cond.dict()
        assert d == {"field": "age", "operator": ">", "value": 30}

    def test_condition_dict_with_in(self):
        fv = FieldValue("country", ["USA", "Canada"])
        cond = Condition(RelationOperator.IN, fv)
        d = cond.dict()
        assert d == {"field": "country", "operator": "IN", "value": ["USA", "Canada"]}

    def test_condition_dict_with_not_in(self):
        fv = FieldValue("status", "inactive")
        cond = Condition(RelationOperator.NOT_IN, fv)
        d = cond.dict()
        assert d == {"field": "status", "operator": "NOT IN", "value": "inactive"}

    def test_condition_dict_with_le(self):
        fv = FieldValue("score", 60)
        cond = Condition(RelationOperator.LE, fv)
        d = cond.dict()
        assert d == {"field": "score", "operator": "<=", "value": 60}

    def test_condition_dict_with_lt(self):
        fv = FieldValue("quantity", 10)
        cond = Condition(RelationOperator.LT, fv)
        d = cond.dict()
        assert d == {"field": "quantity", "operator": "<", "value": 10}

    def test_condition_dict_with_eq(self):
        fv = FieldValue("age", 25)
        cond = Condition(RelationOperator.EQ, fv)
        d = cond.dict()
        assert d == {"field": "age", "operator": "=", "value": 25}

    def test_condition_dict_with_ne(self):
        fv = FieldValue("score", 0)
        cond = Condition(RelationOperator.NE, fv)
        d = cond.dict()
        assert d == {"field": "score", "operator": "!=", "value": 0}


class TestFilter:
    def test_filter_init(self):
        fv = FieldValue("age", 18)
        cond = Condition(RelationOperator.GE, fv)
        flt = Filter(BooleanOperator.AND, [cond])
        assert flt.operator == "AND"
        assert len(flt.conditions) == 1

    def test_filter_dict_with_boolean_operator(self):
        fv1 = FieldValue("age", 18)
        fv2 = FieldValue("country", ["USA", "Canada"])
        cond1 = Condition(RelationOperator.GE, fv1)
        cond2 = Condition(RelationOperator.IN, fv2)
        flt = Filter(BooleanOperator.AND, [cond1, cond2])
        d = flt.dict()
        assert d["operator"] == "AND"
        assert len(d["conditions"]) == 2
        assert d["conditions"][0] == {"field": "age", "operator": ">=", "value": 18}
        assert d["conditions"][1] == {"field": "country", "operator": "IN", "value": ["USA", "Canada"]}

    def test_filter_dict_with_or(self):
        fv1 = FieldValue("status", "active")
        fv2 = FieldValue("role", "admin")
        cond1 = Condition(RelationOperator.GE, fv1)
        cond2 = Condition(RelationOperator.IN, fv2)
        flt = Filter(BooleanOperator.OR, [cond1, cond2])
        d = flt.dict()
        assert d["operator"] == "OR"
        assert len(d["conditions"]) == 2

