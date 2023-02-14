from .boto3 import item_to_dynamodb_item


class FilterExpressionGenerator:
    """
    Generates a string for the FilterExpression argument in some boto3
    functions.
    """
    def __init__(self):
        self.expression = ""
        self.expression_names = {}
        self.expression_values = {}

    def attribute_equals(self, attribute_name, attribute_value):
        self.expression += f"#{attribute_name} = :{attribute_name}Val "
        self.expression_names[f"#{attribute_name}"] = attribute_name
        self.expression_values[f":{attribute_name}Val"] = \
            item_to_dynamodb_item({"key": attribute_value})["key"]
        return self

    def build(self):
        return {
            "FilterExpression": self.expression,
            "ExpressionAttributeNames": self.expression_names,
            "ExpressionAttributeValues": self.expression_values
        }
