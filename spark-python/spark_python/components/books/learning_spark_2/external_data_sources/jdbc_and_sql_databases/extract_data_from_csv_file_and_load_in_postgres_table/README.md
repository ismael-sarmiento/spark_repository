# extract_data_from_csv_file_and_load_in_postgres_table

## The data

### [Omicron daily cases by country (COVID-19 variant)](https://www.kaggle.com/yamqwe/omicron-covid19-variant-daily-cases?select=covid-variants.csv)

* location- this is the country for which the variants information is provided;
* date - date for the data entry;
* variant - this is the variant corresponding to this data entry;
* num_sequences - the number of sequences processed (for the country, variant and date);
* perc_sequences - percentage of sequences from the total number of sequences (for the country, variant and date);
* numsequencestotal - total number of sequences (for the country, variant and date);

#### Table Example

| location | date       |  variant  | num_sequences | perc_sequences | num_sequences_total |
|----------|------------|:---------:|---------------|----------------|---------------------|
| Angola   | 2020-07-06 | B.1.1.277 | 0             | 0.0            | 3                   |
| Angola   | 2020-07-06 | B.1.1.302 | 0             | 0.0            | 3                   |
| Angola   | 2020-07-06 | B.1.1.519 | 0             | 0.0            | 3                   |
| Angola   | 2020-07-06 |  B.1.160  | 0             | 0.0            | 3                   |
| Angola   | 2020-07-06 |  B.1.177  | 0             | 0.0            | 3                   |
| Angola   | 2020-07-06 |  B.1.221  | 0             | 0.0            | 3                   |
| Angola   | 2020-07-06 |  B.1.258  | 0             | 0.0            | 3                   |
| Angola   | 2020-07-06 |  B.1.367  | 0             | 0.0            | 3                   |
| Angola   | 2020-07-06 |  B.1.620  | 0             | 0.0            | 3                   |

#### DDL

```text
pip install ddlgenerator

ddlgenerator postgres covid-variants.csv > covid-variants.sql
```