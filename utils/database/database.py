from multiprocessing import Pool
import configparser
import copy
from utils.parser import csv_to_list
from .KeyConditionGenerator import KeyConditionGenerator
from .boto3 import (
    item_to_dynamodb_item,
    generate_item_key_schema_from_table_key_schema
)
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


def __rename_indexes(rows_data: list, index_renames: list) -> list:
    """
    Todo:
        Rename indexes in place to avoid an entirely new loop.
    """
    renamed_row_data = copy.deepcopy(rows_data)

    for row_index in range(len(rows_data)):
        for key in rows_data[row_index]:
            for index in index_renames:
                if key == index["IndexToRename"]:
                    renamed_row_data[row_index][index["RenameTo"]] = rows_data[row_index][key]
                    del renamed_row_data[row_index][index["IndexToRename"]]
                    break

    return renamed_row_data


def bulk_load_items(
        db: object,
        file_name: str,
        default_table_name: str,
        item_reshaper: callable = None):
    """
    Args:
        item_reshaper (callable): A function with csv_to_list parameter like:
            [{
                "Country": "Costa Rica",
                "Currency": "Costa Rican Colon",
                "1970": 1874394,
                ...
                "2001": 1896077
            }]
            Can be transformed into something like
            [
                {
                    "Country": "Costa Rica",
                    "Year": 1970,
                    "Population": 1874394,
                },
                {
                    "Country": "Costa Rica",
                    "Year": 2001,
                    "Population": 1896077,
                },
                {
                    "Country": "Costa Rica",
                    "Currency": "Costa Rican Colon",
                    "Table": "rshepp02_non_yearly" # Optional
                }
            ]
    """
    csv_row_data = csv_to_list(file_name)

    if item_reshaper:
        csv_row_data = item_reshaper(csv_row_data)

    pool = Pool()
    for row in csv_row_data:
        if "Table" in row:
            pool.apply_async(add_item, (None, row["Table"], row))
        else:
            pool.apply_async(add_item, (None, default_table_name, row))

    pool.close()
    pool.join()


def add_item(db: object, table_name: str, item: dict):
    if db is None:
        db = authenticate("S5-S3.conf")

    new_item = item_to_dynamodb_item(item)
    current_item = None

    try:
        key_schema = generate_item_key_schema_from_table_key_schema(
            db, table_name, item
        )
        current_item = db.get_item(TableName=table_name, Key=key_schema)["Item"]
        db.put_item(
            TableName=table_name,
            Item={**current_item, **new_item})
    except Exception:
        # Current item does not exist
        db.put_item(
            TableName=table_name,
            Item=new_item)

    print(f"Added {item} successfully!")


def delete_item():
    return "Unimplemented"


def display_table():
    return "Unimplemented"


def query(
        db: object,
        table_name: str,
        partition_key_name: str,
        partition_key_val):
    items = None

    key_query_params = KeyConditionGenerator() \
        .key(partition_key_name, partition_key_val) \
        .build()

    items = db.query(
        TableName=table_name,
        KeyConditionExpression=key_query_params["KeyConditionExpression"],
        ExpressionAttributeValues=key_query_params["ExpressionAttributeValues"]
    )["Items"]

    return items


def query_rank():
    return "Unimplmeneted"
