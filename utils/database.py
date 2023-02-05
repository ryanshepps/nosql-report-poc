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


def create_table():
    return "Unimplemented"


def delete_table():
    return "Unimplemented"


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
