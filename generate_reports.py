from modules.database.database import authenticate, query, query_rank, scan
from modules.TemplateCompiler import TemplateCompiler
from modules.database.generators.FilterExpressionGenerator import FilterExpressionGenerator
from copy import deepcopy

COUNTRY = "Costa Rica"


def generate_country_report_context() -> dict:
    db = authenticate("authentication.conf")

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

    single_country_filter = FilterExpressionGenerator() \
        .attribute_equals("Country", COUNTRY) \
        .build()

    population_items = scan(
        db,
        table_name="rshepp02_non_economic",
        generated_filter_expression=single_country_filter,
    )
    context["population_items"] = []
    for population_item in population_items:
        if int(population_item["Population"]) == 0:
            context["population_items"].append({
                "year": population_item["Year"],
                "population": "",
                "population_rank": "",
                "population_density": "",
                "population_density_rank": "",
            })
        else:
            context["population_items"].append({
                "year": population_item["Year"],
                "population": population_item["Population"],
                "population_rank": population_item["Population Rank"],
                "population_density": float(population_item["Population Density"]),
                "population_density_rank": population_item["Population Density Rank"]
            })
    context["population_items"].sort(key=lambda item: int(item["year"]))

    # Trim earliest and latest years that have 0 population (as per spec)
    for population_item in deepcopy((context["population_items"])):
        if population_item["population"] == "":
            context["population_items"].pop(0)
        else:
            break

    for population_item in reversed(deepcopy(context["population_items"])):
        if population_item["population"] == "":
            context["population_items"].pop(-1)
        else:
            break

    gdp_items = scan(
        db,
        table_name="rshepp02_economic",
        generated_filter_expression=single_country_filter
    )
    context["currency"] = gdp_items[0]["Currency"]
    context["gdp_items"] = []
    for gdp_item in gdp_items:
        if int(gdp_item["GDP"]) != 0:
            context["gdp_items"].append({
                "year": gdp_item["Year"],
                "GDPPC": gdp_item["GDP"],
                "GDPPC_rank": gdp_item["GDP Rank"]
            })
        else:
            context["gdp_items"].append({
                "year": "",
                "GDPPC": "",
                "GDPPC_rank": ""
            })

    context["gdp_items"].sort(key=lambda item: int(item["year"]))

    # Trim earliest and latest years that have 0 GDP (as per spec)
    for gdp_item in deepcopy((context["gdp_items"])):
        if gdp_item["GDPPC"] == "":
            context["gdp_items"].pop(0)
        else:
            break

    for gdp_item in reversed(deepcopy((context["gdp_items"]))):
        if gdp_item["GDPPC"] == "":
            context["gdp_items"].pop(-1)
        else:
            break

    context["earliest_economic_year"] = context["gdp_items"][0]["year"]
    context["latest_economic_year"] = context["gdp_items"][-1]["year"]

    return context


TemplateCompiler(
    TemplateRootFolder="./example/report_template/",
    TemplatePath="country_report.html",
    Context=generate_country_report_context()
).compile(f"compiled/{COUNTRY.replace(' ', '')}_report.html")