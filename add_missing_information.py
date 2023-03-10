from modules.database.database import (
    authenticate,
    add_item
)
from utils.deliverable_helpers import (
    rank_population,
    add_population_density,
    rank_population_density
)

db = authenticate("authentication.conf")

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
add_population_density(db)
rank_population_density(db)

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
