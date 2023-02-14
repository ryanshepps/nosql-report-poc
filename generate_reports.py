from copy import deepcopy
from utils.database.database import authenticate, query, query_rank, scan
from utils.database.TemplateCompiler import TemplateCompiler
from utils.database.FilterExpressionGenerator import FilterExpressionGenerator

COUNTRY = "Costa Rica"
YEAR = 2019

db = authenticate("S5-S3.conf")


# ------------------------------- Country Report ------------------------------
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
        context["population_items"].append({
            "year": population_item["Year"],
            "population": population_item["Population"],
            "population_rank": population_item["Population Rank"],
            "population_density": population_item["Population Density"],
            "population_density_rank": population_item["Population Density Rank"]
        })
    context["population_items"].sort(key=lambda item: int(item["year"]))

    gdp_items = scan(
        db,
        table_name="rshepp02_economic",
        generated_filter_expression=single_country_filter
    )
    context["currency"] = gdp_items[0]["Currency"]
    context["gdp_items"] = []
    for gdp_item in gdp_items:
        context["gdp_items"].append({
            "year": gdp_item["Year"],
            "GDPPC": gdp_item["GDP"],
            "GDPPC_rank": gdp_item["GDP Rank"]
        })
    context["gdp_items"].sort(key=lambda item: int(item["year"]))
    context["earliest_economic_year"] = context["gdp_items"][0]["year"]
    context["latest_economic_year"] = context["gdp_items"][-1]["year"]

    return context


TemplateCompiler(
    TemplateRootFolder="./example/report_template/",
    TemplatePath="country_report.html",
    Context=generate_country_report_context()
).compile(f"compiled/{COUNTRY.replace(' ', '')}_report.html")
# -------------------------------------------------------------


def generate_global_report_context() -> dict:
    context = {}
    context["year"] = YEAR

    year_filter = FilterExpressionGenerator() \
        .attribute_equals("Year", YEAR) \
        .build()
    population_items = scan(
        db,
        table_name="rshepp02_non_economic",
        generated_filter_expression=year_filter
    )
    context["population_items"] = []
    for item in population_items:
        if item["Population"] != 0:
            context["population_items"].append({
                "country_name": item["Country"],
                "population": item["Population"],
                "population_rank": item["Population Rank"],
            })
    context["population_items"].sort(key=lambda item: int(item["population_rank"]))

    area_items = scan(
        db,
        table_name="rshepp02_non_yearly",
        include_attributes=["Country", "Area", "Area Rank"]
    )
    context["area_items"] = []
    for item in area_items:
        if item["Area"] != 0:
            context["area_items"].append({
                "country_name": item["Country"],
                "area": item["Area"],
                "area_rank": item["Area Rank"],
            })
    context["area_items"].sort(key=lambda item: int(item["area_rank"]))

    density_items = scan(
        db,
        table_name="rshepp02_non_economic",
        generated_filter_expression=year_filter
    )
    context["density_items"] = []
    for item in density_items:
        if str(item["Population Density"]) != "0.0":
            context["density_items"].append({
                "country_name": item["Country"],
                "density": item["Population Density"],
                "density_rank": item["Population Density Rank"],
            })
    context["density_items"].sort(key=lambda item: int(item["density_rank"]))

    context["num_countries"] = len(population_items)

    return context


TemplateCompiler(
    TemplateRootFolder="./example/report_template/",
    TemplatePath="global_report.html",
    Context=generate_global_report_context()
).compile(f"compiled/{YEAR}_report.html")
