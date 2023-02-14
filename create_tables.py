from modules.database.enums import (
    AttributeType,
    KeyType,
)
from modules.database.database import (
    authenticate,
    bulk_load_items,
    create_table,
    scan,
)
from utils.deliverable_helpers import (
    rank_population,
    rank_population_density,
    rank_gddpc_items
)

db = authenticate("./S5-S3.conf")


# ----------------- Creating Tables -----------------
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
# # --------------------------------------------------------------------

# # ----------------- Loading items from CSV files -----------------
country_currency_map = {}
def reshape_shortlist_curpop(csv_row_data: list):
    new_csv_row_data = []

    for row in csv_row_data:
        # Add Currency to map to be added to economic data later
        country_currency_map[row["Country"]] = row["Currency"]
        del row["Currency"]

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
                "GDP": row[str(year)] if row[str(year)] != "" else 0,
                "Currency": country_currency_map[row["Country"]]
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


def reshape_shortlist_area(csv_row_data: list):
    new_csv_row_data = []

    for row in csv_row_data:
        new_csv_row_data.append({
            "IOS3": row["ISO3"],
            "Country": row["Country Name"],
            "Area": row["Area"]
        })

    return new_csv_row_data


bulk_load_items(
    db,
    file_name="example/input_csv/shortlist_area.csv",
    default_table_name="rshepp02_non_yearly",
    item_reshaper=reshape_shortlist_area)


def reshape_shortlist_capitals(csv_row_data: list):
    new_csv_row_data = []

    for row in csv_row_data:
        new_csv_row_data.append({
            "IOS3": row["ISO3"],
            "Country": row["Country Name"],
            "Capital": row["Capital"]
        })

    return new_csv_row_data


bulk_load_items(
    db,
    file_name="example/input_csv/shortlist_capitals.csv",
    default_table_name="rshepp02_non_yearly",
    item_reshaper=reshape_shortlist_capitals)

bulk_load_items(
    db,
    file_name="example/input_csv/un_shortlist.csv",
    default_table_name="rshepp02_non_yearly")
# --------------------------------------------------------------------

# ----------------- Adding Population Density to table -----------------
area_per_country = scan(
    db,
    table_name="rshepp02_non_yearly",
    include_attributes=["Country", "Area"]
)
country_area_map = {}
for item in area_per_country:
    country_area_map[item["Country"]] = item["Area"]

population_items = scan(
    db,
    table_name="rshepp02_non_economic",
    include_attributes=["Population", "Year", "Country"]
)
population_density_items = []
for item in population_items:
    population_density_items.append({
        "Year": int(item["Year"]),
        "Country": item["Country"],
        "Population Density": str(int(item["Population"]) / int(country_area_map[item["Country"]]))
    })

bulk_load_items(
    db,
    items=population_density_items,
    default_table_name="rshepp02_non_economic"
)
# --------------------------------------------------------------------

rank_population(db)
rank_population_density(db)
rank_gddpc_items(db)
