from multiprocessing import Pool
import configparser
import copy
from modules.parser import csv_to_list
from .generators.KeyConditionGenerator import KeyConditionGenerator
from .generators.ProjectionExpressionGenerator import ProjectionExpressionGenerator
from .apis.boto3.helpers import (
    item_to_dynamodb_item,
    dynamo_db_item_to_item,
    generate_item_key_schema_from_table_key_schema
)
import boto3
from tabulate import tabulate


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


def bulk_load_items(
        db: object,
        default_table_name: str,
        file_name: str = None,
        items: list = None,
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
    items_to_add = None

    if (file_name is None and items is None):
        raise Exception("Must provide either file_name OR items")
    elif file_name is not None:
        items_to_add = csv_to_list(file_name)
    elif items is not None:
        items_to_add = items
    else:
        raise Exception("file_name OR items not provided")

    if item_reshaper is not None:
        items_to_add = item_reshaper(items_to_add)

    pool = Pool()
    for row in items_to_add:
        if "Table" in row:
            table = row["Table"]
            del row["Table"]  # Don't actually put Table as an item attribute
            pool.apply_async(add_item, (None, table, row))
        else:
            pool.apply_async(add_item, (None, default_table_name, row))

    pool.close()
    pool.join()


def add_item(db: object, table_name: str, item: dict):
    if db is None:
        db = authenticate("authentication.conf")

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


def delete_item(db: object, table_name: str, item: dict):
    dynamo_db_item = item_to_dynamodb_item(item)

    db.delete_item(
        TableName=table_name,
        Key=dynamo_db_item
    )


def display_table(
        db,
        table_name: str,
        include_attributes: list = None,
        generated_filter_expression: str = None):
    table_items = scan(
        db,
        table_name=table_name,
        include_attributes=include_attributes,
        generated_filter_expression=generated_filter_expression
    )

    print(tabulate(table_items, headers="keys"))


def query(
        db: object,
        table_name: str,
        partition_key_name: str,
        partition_key_val):
    items = []

    key_query_params = KeyConditionGenerator() \
        .key(partition_key_name, partition_key_val) \
        .build()

    dynamo_db_items = db.query(
        TableName=table_name,
        KeyConditionExpression=key_query_params["KeyConditionExpression"],
        ExpressionAttributeValues=key_query_params["ExpressionAttributeValues"]
    )["Items"]

    for dynamo_db_item in dynamo_db_items:
        items.append(dynamo_db_item_to_item(dynamo_db_item))

    return items


def scan(
        db: object,
        table_name: str,
        include_attributes: list = None,
        generated_filter_expression: str = None,
        item_reshaper: callable = None):
    dynamo_db_items = None

    scan_args = {}
    scan_args["TableName"] = table_name

    if include_attributes is not None:
        generated_included_attributes = ProjectionExpressionGenerator() \
            .attribute_list(include_attributes) \
            .build()
        scan_args["ProjectionExpression"] = generated_included_attributes["ProjectionExpression"]
        scan_args["ExpressionAttributeNames"] = generated_included_attributes["ExpressionAttributeNames"]
    if generated_filter_expression is not None:
        scan_args["FilterExpression"] = \
            generated_filter_expression["FilterExpression"]
        scan_args["ExpressionAttributeValues"] = \
            generated_filter_expression["ExpressionAttributeValues"]

        if "ExpressionAttributeNames" not in scan_args:
            scan_args["ExpressionAttributeNames"] = \
                generated_filter_expression["ExpressionAttributeNames"]
        else:
            scan_args["ExpressionAttributeNames"] = {
                **generated_filter_expression["ExpressionAttributeNames"],
                **scan_args["ExpressionAttributeNames"]
            }

    dynamo_db_items = db.scan(
        **scan_args
    )["Items"]

    items = []
    for dynamo_db_item in dynamo_db_items:
        items.append(dynamo_db_item_to_item(dynamo_db_item))

    if item_reshaper is not None:
        items = item_reshaper(items)

    return items


def query_rank(
        db: object,
        table_name: str,
        partition_key_name: str,
        partition_key_val: str,
        attribute_to_rank: str):
    def sort_by_attribute_to_rank(items):
        items.sort(key=lambda item: int(item[attribute_to_rank]), reverse=True)
        return items

    items = scan(
        db,
        table_name=table_name,
        include_attributes=[partition_key_name, attribute_to_rank],
        item_reshaper=sort_by_attribute_to_rank
    )

    for rank, item in enumerate(items, start=1):
        if item[partition_key_name] == partition_key_val:
            return rank
