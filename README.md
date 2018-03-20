# Parking Violations Data Pipeline

Takes raw data from parking ticket database exports and:

- Geocodes using [Passyunk](https://github.com/CityOfPhiladelphia/passyunk)
- Anonymizes the driver's license plate
- Anonymizes the ticket number

Note: Parking tickets and plates CSVs are backed up in s3://phl-data-dropbox/sftp/Parking_Violations/. Centroids file can be obtained from CityGeo.

Files can be local or use `s3://{bucket}/{key}` notation to access S3 files.

### Usage

```sh
cat input_raw_data.txt | python main.py --plates-file plates.csv --ticket-numbers-file tickets.csv --centroid-file street_centroid.csv
```

If this is an old file, with not latitude and longitude columns, use:

```sh
cat input_raw_data.txt | python main.py --plates-file plates.csv --ticket-numbers-file tickets.csv --centroid-file street_centroid.csv --no-latlon-input
```
