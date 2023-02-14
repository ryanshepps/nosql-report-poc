from modules.database.database import (
    scan,
    bulk_load_items
)


# TODO - Make this more reusable with other rank functions
def rank_population(db):
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

    bulk_load_items(
        db,
        items=new_non_economic_items,
        default_table_name="rshepp02_non_economic"
    )


def rank_population_density(db):
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

    bulk_load_items(
        db,
        items=new_non_economic_items,
        default_table_name="rshepp02_non_economic"
    )


def rank_gddpc_items(db):
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

    bulk_load_items(
        db,
        items=new_economic_items,
        default_table_name="rshepp02_economic"
    )
