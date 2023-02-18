# NoSQL Report

## Installation

### Limitations

This has been tested with `Python 3.9.13` and `pip 21.0.1` on Linux. It may work with earlier/later versions and on other operating systems but is only gauranteed to work with those versions and on Linux.

Run to install all required dependencies:

```
pip install -r requirements.txt
```

## Running

Fill in `authentication.conf` with your AWS access key ID and secret.

To create the tables and fill them with the example csv files, run

```bash
python create_tables.py
```

To fill the tables with the information they are missing, run

```bash
python add_missing_information.py
```

To generate a country's report, fill in the COUNTRY variable at the top of `generate_reports.py` with the country you want to generate the report for and run. The specified report I chose to generate is for a specified country (Report A).

```bash
python generate_reports.py
```

#### **Making Edits to Tables**

There are examples of how to make edits to database tables in `create_tables.py` and `add_missing_information.py`. The functions may contain important docstrings where parameters may be confusing, but there will always be an example of how to use a function that may have unobvious parameters.

For creating, reading, updating and deleting items, the modules are lisetd below.

## Report Requirements

### Tables

1. Tables are designed so that more data can be added to them by country/year.
1. Non-economic and economic data data are not in the same table.

The table schema looks like this:

**rshepp02_non_yearly**

| Country | Area | Capital | ISO3 | ISO2 | Languages | Official Name |
| - | - | - | - | - | - | - |
| Partition Key |

**rshepp02_non_economic**

| Year | Country | Population | Population Density | Population Density Rank | Population Rank |
| - | - | - | - | - | - |
| Partition Key | Partition Sort Key |

**rshepp02_economic**

| Country | Year | Currency | GDP | GDP Rank |
| - | - | - | - | - |
| Partition Key | Partition Sort Key |

### Modules

Each module resides in `./modules/database/database.py`. Examples of how to import and use modules can be found in `create_tables.py`, `add_missing_information.py` and `generate_reports.py`.

Modules exist for:

#### **authenticate**

```python
database = authenticate(
    relative_config_location: str,
    region: str = 'us-east-2'
)
```

Returns an authenticated DynamoDB client if credentials are correct.

#### **create_table**

```python
create_table(
    db: object,
    table_name: str,
    key_schema: list
)
```

#### **delete_table**

```python
delete_table(
    db: object,
    table_name: str
):
```

#### **bulk_load_items**

```python
bulk_load_items(
    db: object,
    default_table_name: str,
    file_name: str = None,
    items: list = None,
    item_reshaper: callable = None
)
```

#### **add_item**

```python
add_item(
    db: object,
    table_name: str,
    item: dict
)
```

#### **delete_item**

```python
delete_item(
    db: object,
    table_name: str,
    item: dict
)
```

#### **display_table**

```python
display_table(
    db: object,
    table_name: str,
    include_attributes: list = None,
    generated_filter_expression: str = None
)
```

#### **query**

```python
items = query(
        db: object,
        table_name: str,
        partition_key_name: str,
        partition_key_val
)
```

Returns a list of human readable items.

#### **scan**

```python
items = scan(
    db: object,
    table_name: str,
    include_attributes: list = None,
    generated_filter_expression: str = None,
    item_reshaper: callable = None
)
```

Returns a list of human readable items.

#### **query_rank**

```python
rank query_rank(
    db: object,
    table_name: str,
    partition_key_name: str,
    partition_key_val: str,
    attribute_to_rank: str
)
```

Returns the rank of `attribute_to_rank` for `partition_key_name` and `partition_key_val`.

### Scripts

**`create_tables.py`**

Creates the aforementioned tables and asynchronously fills them with the example csv files using the aforementioned modules. Also ranks some attributes per year in preparation for generating reports later.

**`add_missing_information.py`**

Demonstrates how to use some of the modules to add information that may be missing from csv files. Asynchronously reranks each item based on the new items added.

**`generate_reports.py`**

Queries the tables created in `create_tables.py` and compiles an HTML template into a report based on requirements. To generate these reports, see "Running" above.