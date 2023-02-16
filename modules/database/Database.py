import configparser
from multiprocessing import Pool
from modules.parser import csv_to_list
import boto3
from .apis.boto3.helpers import (
    item_to_dynamodb_item,
    generate_item_key_schema_from_table_key_schema
)


class Database():
    def __init__(self):
        self.db = self.authenticate("./authentication.conf")

    def authenticate(
            self,
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

    def bulk_load_items(
            self,
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
                pool.apply_async(self.add_item, args=(table, row))
            else:
                pool.apply_async(self.add_item, args=(default_table_name, row))

        pool.close()
        pool.join()

    def add_item(self, table_name: str, item: dict):
        new_item = item_to_dynamodb_item(item)
        current_item = None

        try:
            key_schema = generate_item_key_schema_from_table_key_schema(
                self.db, table_name, item
            )
            current_item = self.db.get_item(TableName=table_name, Key=key_schema)["Item"]
            self.db.put_item(
                TableName=table_name,
                Item={**current_item, **new_item})
        except Exception as e:
            print(e)
            # Current item does not exist
            self.db.put_item(
                TableName=table_name,
                Item=new_item)

        print(f"Added {item} successfully!")
