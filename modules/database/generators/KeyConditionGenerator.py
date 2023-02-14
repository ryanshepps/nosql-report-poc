from ..apis.boto3.helpers import item_to_dynamodb_item


class KeyConditionGenerator:
    """
    Generates a string for KeyConditionExpressonn in boto3.client.query
    """
    def __init__(self):
        self.expression = ""
        self.expression_names = {}

    def key(self, key_name, key_value):
        self.expression += f"{key_name} = :{key_name}Val"
        self.expression_names[f":{key_name}Val"] = \
            item_to_dynamodb_item({"key": key_value})["key"]
        return self

    def build(self):
        return {
            "KeyConditionExpression": self.expression,
            "ExpressionAttributeValues": self.expression_names,
        }
