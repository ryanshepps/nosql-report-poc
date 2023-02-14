from utils.database.database import (
    authenticate,
    add_item
)
from deliverable_helpers import (
    rank_population
)

db = authenticate("S5-S3.conf")

add_item(
    db,
    "rshepp02_non_economic",
    {
        "Country": "Australia",
        "Year": 2019,
        "Population": 25203198,
    }
)

add_item(
    db,
    "rshepp02_non_economic",
    {
        "Country": "Bangladesh",
        "Year": 2019,
        "Population": 163046161,
    }
)

add_item(
    db,
    "rshepp02_non_economic",
    {
        "Country": "Canada",
        "Year": 2019,
        "Population": 37411047,
    }
)

add_item(
    db,
    "rshepp02_non_economic",
    {
        "Country": "Costa Rica",
        "Year": 2019,
        "Population": 5047561,
    }
)

rank_population(db)

add_item(
    db,
    "rshepp02_non_yearly",
    {
        "Country": "Comoros",
        "Language": "French",
    }
)

add_item(
    db,
    "rshepp02_non_yearly",
    {
        "Country": "Cook Islands",
        "Language": "English",
    }
)
