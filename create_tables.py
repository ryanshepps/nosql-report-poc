from enums.database import (
    AttributeType,
    KeyType,
)
from utils.database.database import (
    authenticate,
    bulk_load_items,
    create_table,
)

db = authenticate("./S5-S3.conf")


create_table(
    db,
    table_name="rshepp02_non_yearly",
    key_schema=[{
        "AttributeName": "Country",
        "AttributeType": AttributeType.STRING,
        "KeyType": KeyType.PARTITION_KEY
    }]
)

create_table(
    db,
    table_name="rshepp02_non_economic",
    key_schema=[
        {
            "AttributeName": "Year",
            "AttributeType": AttributeType.NUMBER,
            "KeyType": KeyType.PARTITION_KEY
        },
        {
            "AttributeName": "Country",
            "AttributeType": AttributeType.STRING,
            "KeyType": KeyType.PARTITION_SORT_KEY
        },
    ]
)

create_table(
    db,
    table_name="rshepp02_economic",
    key_schema=[
        {
            "AttributeName": "Country",
            "AttributeType": AttributeType.STRING,
            "KeyType": KeyType.PARTITION_KEY
        },
        {
            "AttributeName": "Year",
            "AttributeType": AttributeType.NUMBER,
            "KeyType": KeyType.PARTITION_SORT_KEY
        },
    ]
)


def reshape_shortlist_curpop(csv_row_data: list):
    new_csv_row_data = []

    for row in csv_row_data:
        # Add Currency to rshepp02_non_yearly
        new_csv_row_data.append({
            "Country": row["Country"],
            "Currency": row["Currency"],
            "Table": "rshepp02_non_yearly"
        })

        # Create a new row for each year
        for year in range(1970, 2020):
            new_csv_row_data.append({
                "Year": year,
                "Country": row["Country"],
                "Population": row[str(year)] if row[str(year)] != "" else 0,
            })

    return new_csv_row_data


bulk_load_items(
    db,
    file_name="example/input_csv/shortlist_curpop.csv",
    default_table_name="rshepp02_non_economic",
    item_reshaper=reshape_shortlist_curpop)


def reshape_shortlist_gdppc(csv_row_data: list):
    new_csv_row_data = []

    for row in csv_row_data:
        for year in range(1970, 2020):
            new_csv_row_data.append({
                "Country": row["Country"],
                "Year": year,
                "GDP": row[str(year)] if row[str(year)] != "" else 0
            })

    return new_csv_row_data


bulk_load_items(
    db,
    file_name="example/input_csv/shortlist_gdppc.csv",
    default_table_name="rshepp02_economic",
    item_reshaper=reshape_shortlist_gdppc)


def reshape_shortlist_languages(csv_row_data: list):
    new_csv_row_data = []

    for row in csv_row_data:
        new_csv_row_data.append({
            "ISO3": row["ISO3"],
            "Country": row["Country Name"],
            "Languages": row["Languages"]
        })

    return new_csv_row_data


bulk_load_items(
    db,
    file_name="example/input_csv/shortlist_languages.csv",
    default_table_name="rshepp02_non_yearly",
    item_reshaper=reshape_shortlist_languages)


bulk_load_items(
    db,
    file_name="example/input_csv/un_shortlist.csv",
    default_table_name="rshepp02_non_yearly")
