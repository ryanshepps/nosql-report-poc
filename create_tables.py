from utils.database import authenticate, bulk_load_items

db = authenticate("./S5-S3.conf")


def reshape_shortlist_curpop(csv_row_data: list):
    new_csv_row_data = []

    for row in csv_row_data:
        # Add Currency to rshepp02_non_yearly
        new_csv_row_data.append({
            "Country": row["Country"],
            "Currency": row["Currency"],
            "Table": "rshepp02_non_yearly"
        })

        # Create a new row for each year
        for year in range(1970, 2020):
            new_csv_row_data.append({
                "Country": row["Country"],
                "Year": year,
                "Population": row[str(year)] if row[str(year)] != "" else 0,
            })

    return new_csv_row_data


bulk_load_items(
    db,
    file_name="example/input_csv/shortlist_curpop.csv",
    default_table_name="rshepp02_non_economic",
    item_reshaper=reshape_shortlist_curpop)
