from typing import List, Union


class RelationOperator:
    IN = "IN"
    # TODO EQ = "="
    GT = ">"
    GE = ">="
    LT = "<"
    LE = "<="
    # TODO LIKE = "LIKE"


class BooleanOperator:
    AND = "AND"
    OR = "OR"


BooleanOperators = [BooleanOperator.AND, BooleanOperator.OR]


class FieldValue(object):
    def __init__(self, field: str, value: Union[float, int, List, str]):
        self.field = field
        self.value = value


class Condition(object):
    def __init__(self, operator: str, fv: FieldValue):
        self.fv = fv
        self.relation_operator = operator

    def dict(self):
        return {"field": self.fv.field, "operator": self.relation_operator, "value": self.fv.value}


class Conditions(object):
    def __init__(self, operator: str, conditions: List[Condition]):
        self.operator = operator
        self.conditions = conditions

    def dict(self):
        conditions_dict = [condition.dict() for condition in self.conditions]
        return {"operator": self.operator, "conditions": conditions_dict}


class Filter(object):
    """
    import json

    def apply_filter(data, filter_expr):
        operator = filter_expr.get("operator")
        conditions = filter_expr.get("conditions")

        if operator == "AND":
            return all(apply_filter(data, condition) for condition in conditions)
        elif operator == "OR":
            return any(apply_filter(data, condition) for condition in conditions)
        else:
            field = filter_expr.get("field")
            operator = filter_expr.get("operator")
            value = filter_expr.get("value")

            # Apply the filter logic based on the operator
            if operator == ">":
                return data.get(field) > value
            elif operator == ">=":
                return data.get(field) >= value
            elif operator == "=":
                return data.get(field) == value
            elif operator == "LIKE":
                return value in data.get(field)
            elif operator == "IN":
                return data.get(field) in value
            else:
                return False

    # Sample data
    data = {
        "age": 25,
        "country": "USA",
        "gender": "Male",
        "occupation": "Software Engineer"
    }

    # Sample filter expression
    filter_expr = {
        "operator": "AND",
        "conditions": [
            {
                "operator": ">",
                "field": "age",
                "value": 18
            },
            {
                "operator": "IN",
                "field": "country",
                "value": ["USA", "Canada", "UK"]
            },
            {
                "operator": "OR",
                "conditions": [
                    {
                        "operator": "=",
                        "field": "gender",
                        "value": "Male"
                    },
                    {
                        "operator": "LIKE",
                        "field": "occupation",
                        "value": "Engineer"
                    }
                ]
            }
        ]
    }

    # Apply the filter to the data
    result = apply_filter(data, filter_expr)
    print(result)  # True if the data satisfies the filter condition, False otherwise
    """

    def __init__(self, operator: str, conditions: Union[FieldValue, Union[Condition, Conditions]]):
        self.operator = operator
        self.conditions = conditions

    def dict(self):
        if self.operator not in BooleanOperators:
            return {"operator": self.operator, "field": self.conditions.field, "value": self.conditions.value}
        conditions_dict = [condition.dict() for condition in self.conditions]
        return {"operator": self.operator, "conditions": conditions_dict}
