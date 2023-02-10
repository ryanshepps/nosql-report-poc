def item_to_dynamodb_item(item: dict) -> dict:
    dynamodb_item = {}

    for key in item:
        if type(item[key]) is int:
            dynamodb_item[key] = {
                "N": str(item[key])  # Numbers need to be converted to strings ;'(
            }
        else:
            dynamodb_item[key] = {
                "S": item[key]
            }

    return dynamodb_item


def dynamo_db_item_to_item(dynamo_db_item: dict) -> dict:
    item = {}

    for key in dynamo_db_item:
        if "S" in dynamo_db_item[key]:
            item[key] = dynamo_db_item[key]["S"]
        elif "N" in dynamo_db_item[key]:
            item[key] = dynamo_db_item[key]["N"]
        else:
            item[key] = dynamo_db_item[key]

    return item


def generate_item_key_schema_from_table_key_schema(
        db,
        table_name,
        item):
    item_key_schema = {}
    table_key_schema = db.describe_table(TableName=table_name)["Table"]["KeySchema"]

    for table_key in table_key_schema:
        current_key_schema = item_to_dynamodb_item({
            table_key["AttributeName"]: item[table_key["AttributeName"]]
        })
        item_key_schema[table_key["AttributeName"]] = current_key_schema[table_key["AttributeName"]]

    return item_key_schema
