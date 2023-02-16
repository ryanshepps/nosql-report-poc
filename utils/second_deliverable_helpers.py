from modules.database.database import (
    scan,
    bulk_load_items
)


# TODO - Make this more reusable with other rank functions
def rank_population(db, database):
    non_economic_items = scan(
        db,
        table_name="rshepp02_non_economic"
    )

    grouped_by_year_population_items = {}
    for item in non_economic_items:
        item_year = item["Year"]

        if item_year not in grouped_by_year_population_items:
            grouped_by_year_population_items[item_year] = []

        grouped_by_year_population_items[item_year].append(item)

    # Rank each item in each group
    new_non_economic_items = []
    for year_group in grouped_by_year_population_items:
        grouped_by_year_population_items[year_group] \
            .sort(key=lambda item: int(item["Population"]), reverse=True)

        for rank, item in enumerate(grouped_by_year_population_items[year_group], start=1):
            item["Population Rank"] = rank

        new_non_economic_items.extend(grouped_by_year_population_items[year_group])

    database.bulk_load_items(
        items=new_non_economic_items,
        default_table_name="rshepp02_non_economic"
    )


def rank_population_density(db, database):
    non_economic_items = scan(
        db,
        table_name="rshepp02_non_economic"
    )

    grouped_by_year_population_items = {}
    for item in non_economic_items:
        item_year = item["Year"]

        if item_year not in grouped_by_year_population_items:
            grouped_by_year_population_items[item_year] = []

        grouped_by_year_population_items[item_year].append(item)

    new_non_economic_items = []
    for year_group in grouped_by_year_population_items:
        grouped_by_year_population_items[year_group] \
            .sort(key=lambda item: float(item["Population Density"]), reverse=True)

        for rank, item in enumerate(grouped_by_year_population_items[year_group], start=1):
            item["Population Density Rank"] = rank

        new_non_economic_items.extend(grouped_by_year_population_items[year_group])

    database.bulk_load_items(
        items=new_non_economic_items,
        default_table_name="rshepp02_non_economic"
    )


def rank_gddpc_items(db, database):
    economic_items = scan(
        db,
        table_name="rshepp02_economic"
    )

    grouped_by_year_economic_items = {}
    for item in economic_items:
        item_year = item["Year"]

        if item_year not in grouped_by_year_economic_items:
            grouped_by_year_economic_items[item_year] = []

        grouped_by_year_economic_items[item_year].append(item)

    # Rank each item in each group
    new_economic_items = []
    for year_group in grouped_by_year_economic_items:
        grouped_by_year_economic_items[year_group] \
            .sort(key=lambda item: int(item["GDP"]), reverse=True)

        for rank, item in enumerate(grouped_by_year_economic_items[year_group], start=1):
            item["GDP Rank"] = rank

        new_economic_items.extend(grouped_by_year_economic_items[year_group])

    database.bulk_load_items(
        items=new_economic_items,
        default_table_name="rshepp02_economic"
    )


def add_population_density(db, database):
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

    database.bulk_load_items(
        items=population_density_items,
        default_table_name="rshepp02_non_economic"
    )
