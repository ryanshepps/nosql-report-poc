from utils.database.database import authenticate, query
from utils.database.boto3 import dynamo_db_item_to_item
from utils.database.TemplateCompiler import TemplateCompiler

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
    )
    non_yearly_country_data = \
        dynamo_db_item_to_item(non_yearly_country_data[0])

    context["name"] = non_yearly_country_data["Country"]
    context["official_name"] = non_yearly_country_data["Official Name"]
    context["area"] = non_yearly_country_data["Area"]
    context["languages"] = non_yearly_country_data["Languages"]
    context["capital"] = non_yearly_country_data["Capital"]

    return context


TemplateCompiler(
    TemplateRootFolder="./example/report_template/",
    TemplatePath="country_report.html",
    Context=generate_country_report_context()
).compile(f"compiled/{COUNTRY.replace(' ', '')}_report.html")
