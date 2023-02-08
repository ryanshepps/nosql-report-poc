import csv


def csv_to_list(file_location: str) -> list:
    csv_file_rows = []

    with open(file_location, encoding="utf-8-sig") as csv_file:
        csv_file_reader = csv.DictReader(csv_file)

        for row in csv_file_reader:
            typed_row = {}
            for key in row:
                try:
                    typed_row[key] = int(row[key])
                except Exception:
                    typed_row[key] = str(row[key])

            csv_file_rows.append(typed_row)

    csv_file.close()
    return csv_file_rows
