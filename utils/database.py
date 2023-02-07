import configparser
import boto3


def authenticate(
        relative_config_location: str,
        region: str = "us-east-2") -> object:
    aws_config = configparser.ConfigParser()
    aws_config.read(relative_config_location)
    return boto3.client(
        "dynamodb",
        aws_access_key_id=aws_config.get("default", "aws_access_key_id"),
        aws_secret_access_key=aws_config.get("default", "aws_secret_access_key"),
        region_name=region
    )


def __find_partition_key(key_schema):
    try:
        return next(key for key in key_schema if str(key["KeyType"] == "PartitionKey"))
    except Exception:
        raise Exception("Partition Key could not be found. Please provide an attribute with the KeyType 'ParititonKey'")


def create_table(
        db: object,
        table_name: str,
        key_schema: list):
    """
    Args:
        key_schema (list): A schema for the table. For example:
            [{
                "AttributeName": "str",
                "AttributeType": "S|N|B",
                "KeyType": "SortKey|PartitionKey|PartitionSortKey",
            }]
    """
    partition_key = __find_partition_key(key_schema)

    new_table_attribute_definitions = []
    new_table_key_schema = []
    new_table_local_secondary_indexes = []

    for key in key_schema:
        new_table_attribute_definitions.append({
            "AttributeName": str(key["AttributeName"]),
            "AttributeType": str(key["AttributeType"]),
        })

        if str(key["KeyType"]) == "PartitionKey":
            new_table_key_schema.append({
                "AttributeName": str(key["AttributeName"]),
                "KeyType": "HASH"
            })
        elif str(key["KeyType"]) == "PartitionSortKey":
            new_table_key_schema.append({
                "AttributeName": str(key["AttributeName"]),
                "KeyType": "RANGE"
            })
        elif str(key["KeyType"]) == "SortKey":
            new_table_local_secondary_indexes.append({
                "IndexName": f"{str(key['AttributeName'])}-SortKey",
                "KeySchema": [
                    {
                        "AttributeName": str(partition_key["AttributeName"]),
                        "KeyType": "HASH"
                    },
                    {
                        "AttributeName": str(key["AttributeName"]),
                        "KeyType": "RANGE"
                    }
                ],
                "Projection": {
                    "ProjectionType": "ALL"
                }
            })

    print(f"Creating Table {table_name}...")
    if len(new_table_local_secondary_indexes) > 0:
        db.create_table(
            TableName=table_name,
            AttributeDefinitions=new_table_attribute_definitions,
            KeySchema=new_table_key_schema,
            LocalSecondaryIndexes=new_table_local_secondary_indexes,
            BillingMode="PAY_PER_REQUEST"
        )
    else:
        db.create_table(
            TableName=table_name,
            AttributeDefinitions=new_table_attribute_definitions,
            KeySchema=new_table_key_schema,
            BillingMode="PAY_PER_REQUEST"
        )
    waiter = db.get_waiter("table_exists")
    waiter.wait(TableName=table_name)
    print(f"Table {table_name} created.")


def delete_table(db: object, table_name: str):
    print(f"Deleting table {table_name}...")
    db.delete_table(
        TableName=table_name
    )
    waiter = db.get_waiter("table_not_exists")
    waiter.wait(TableName=table_name)
    print(f"Table {table_name} deleted.")


def bulk_load_items():
    return "Unimplemented"


def add_item():
    return "Unimplemented"


def delete_item():
    return "Unimplemented"


def display_table():
    return "Unimplemented"


def query():
    return "Unimplemented"


def query_rank():
    return "Unimplmeneted"
