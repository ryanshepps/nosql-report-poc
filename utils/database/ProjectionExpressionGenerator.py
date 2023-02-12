class ProjectionExpressionGenerator:
    """
    Generates a string for ProjectionExpression in boto3.client
    """
    def __init__(self):
        self.expression = ""
        self.expression_names = {}

    def attribute_list(self, attribute_names: list):
        for attribute in attribute_names:
            self.attribute(attribute)

        return self

    def attribute(self, attribute_name):
        self.expression += f"#{attribute_name}Val, "
        self.expression_names[f"#{attribute_name}Val"] = attribute_name
        return self

    def build(self):
        return {
            "ProjectionExpression": self.expression[:-2],  # Reomves ", " from end of expression
            "ExpressionAttributeNames": self.expression_names
        }
