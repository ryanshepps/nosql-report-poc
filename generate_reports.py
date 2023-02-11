from copy import deepcopy
from utils.database.database import authenticate, query, query_rank, scan
from utils.database.TemplateCompiler import TemplateCompiler
from utils.database.FilterExpressionGenerator import FilterExpressionGenerator

COUNTRY = "Costa Rica"
YEAR = "2019"

db = authenticate("S5-S3.conf")


def generate_country_report_context() -> dict:
    context = {}

    non_yearly_country_data = query(
        db,
        table_name="rshepp02_non_yearly",
        partition_key_name="Country",
        partition_key_val=COUNTRY
    )[0]

    context["name"] = non_yearly_country_data["Country"]
    context["official_name"] = non_yearly_country_data["Official Name"]
    context["area"] = non_yearly_country_data["Area"]
    context["languages"] = non_yearly_country_data["Languages"]
    context["capital"] = non_yearly_country_data["Capital"]
    context["area_rank"] = query_rank(
        db, "rshepp02_non_yearly", "Country", COUNTRY, "Area")

    country_area = non_yearly_country_data["Area"]  # For popluation density calculations

    def __add_population_and_density_rank(items):
        # Add Density
        for item in items:
            item["population_density"] = int(item["Population"]) / int(country_area)

        population_sorted_items = deepcopy(items)
        population_sorted_items.sort(key=lambda item: int(item["Population"]), reverse=True)

        density_sorted_items = deepcopy(items)
        density_sorted_items.sort(key=lambda item: int(item["population_density"]), reverse=True)

        # Add ranks
        for item in items:
            for rank, pop_sorted_item in enumerate(population_sorted_items, start=1):
                if pop_sorted_item["Population"] == item["Population"]:
                    item["population_rank"] = rank
                    break

            for rank, density_sorted_item in enumerate(density_sorted_items, start=1):
                if density_sorted_item["population_density"] == item["population_density"]:
                    item["population_density_rank"] = rank
                    break

        # Order by year
        items.sort(key=lambda item: int(item["Year"]))

        return items

    single_country_filter = FilterExpressionGenerator() \
        .attribute_equals("Country", COUNTRY) \
        .build()
    population_items = scan(
        db,
        table_name="rshepp02_non_economic",
        generated_filter_expression=single_country_filter,
        item_reshaper=__add_population_and_density_rank
    )

    context["population_items"] = []
    for population_item in population_items:
        context["population_items"].append({
            "year": population_item["Year"],
            "population": population_item["Population"],
            "population_rank": population_item["population_rank"],
            "population_density": population_item["population_density"],
            "population_density_rank": population_item["population_density_rank"]
        })

    return context


TemplateCompiler(
    TemplateRootFolder="./example/report_template/",
    TemplatePath="country_report.html",
    Context=generate_country_report_context()
).compile(f"compiled/{COUNTRY.replace(' ', '')}_report.html")
