import csv


def csv_to_dict(file_location: str) -> dict:
    csv_file_rows = []

    with open(file_location, encoding="utf-8") as csv_file:
        csv_file_reader = csv.DictReader(csv_file)

        for row in csv_file_reader:
            csv_file_rows.append(row)

    csv_file.close()
    return csv_file_rows
