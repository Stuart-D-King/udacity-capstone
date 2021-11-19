### Data Dictionary

**immigration**

| column name    | data type | description | origin |
| -------------- | --------- | ----------- | ------ |
| immigration_id | long | monotonically increasing and unique ID | PySpark generated |
| month | integer | integer representation of month of the year | 2016 I-94 immigration data |
| airport_cd | string | IATA airport code | 2016 I-94 immigration data |
| gender | map (key: string, value: integer) | visit counts by gender in key value pairs | 2016 I-94 immigration data |
| avg_stay | double | average length of stay in days | 2016 I-94 immigration data |
| median_age | double | median age of visitors | 2016 I-94 immigration data |
| trip_purpose | map (key: string, value: integer) | visit counts by trip purpose in key value pairs | 2016 I-94 immigration data |
| visa_type | map (key: string, value: integer) | visit counts by visa type in key value pairs | 2016 I-94 immigration data |
| countries | map (key: string, value: integer) | visit counts by country of origin in key value pairs | 2016 I-94 immigration data |
| airlines | map (key: string, value: integer) | visit counts by airline in key value pairs | 2016 I-94 immigration data |


**airport**

| column name    | data type | description | origin |
| -------------- | --------- | ----------- | ------ |
| airport_cd | string | IATA airport code | airport location information and attributes dataset from datahub.io |
| type | string | type of airport (small, medium, large) | airport location information and attributes dataset from datahub.io |
| name | string | airport name | airport location information and attributes dataset from datahub.io |
| state | string | two letter U.S. state code of airport location | airport location information and attributes dataset from datahub.io |
| city | string | U.S. city of airport location | airport location information and attributes dataset from datahub.io |
| latitude | double | latitudinal coordinate of airport location | airport location information and attributes dataset from datahub.io |
| longitude | double | longitudinal coordinate of airport location | airport location information and attributes dataset from datahub.io |


**demographics**

| column name    | data type | description | origin |
| -------------- | --------- | ----------- | ------ |
| city | string | U.S. city name | city demographics data from the U.S. Census Bureau's 2015 American Community Survey |
| state | string | two letter U.S. state code | city demographics data from the U.S. Census Bureau's 2015 American Community Survey |
| median_age | double | median age of city resident | city demographics data from the U.S. Census Bureau's 2015 American Community Survey |
| male_pop | integer | male population | city demographics data from the U.S. Census Bureau's 2015 American Community Survey |
| female_pop | integer | female population | city demographics data from the U.S. Census Bureau's 2015 American Community Survey |
| native_am_pop | integer | native american population | city demographics data from the U.S. Census Bureau's 2015 American Community Survey |
| asian_pop | integer | asian population | city demographics data from the U.S. Census Bureau's 2015 American Community Survey |
| black_pop | integer | african american population | city demographics data from the U.S. Census Bureau's 2015 American Community Survey |
| hispanic_pop | integer | hispanic population | city demographics data from the U.S. Census Bureau's 2015 American Community Survey |
| white_pop | integer | caucasian population | city demographics data from the U.S. Census Bureau's 2015 American Community Survey |
| veterans | integer | number of veterans | city demographics data from the U.S. Census Bureau's 2015 American Community Survey |
| foreign_born | integer | number of foreign born residents | city demographics data from the U.S. Census Bureau's 2015 American Community Survey |
| avg_hh_size | double | average household size | city demographics data from the U.S. Census Bureau's 2015 American Community Survey |


**temperature**

| column name    | data type | description | origin |
| -------------- | --------- | ----------- | ------ |
| month | integer | integer representation of month of the year | 1950-2013 temperature data from the Berkeley Earth Surface Temperature Study |
| city | string | U.S. city name | 1950-2013 temperature data from the Berkeley Earth Surface Temperature Study |
| latitude | double | latitudinal coordinate of city | 1950-2013 temperature data from the Berkeley Earth Surface Temperature Study |
| longitude | double | longitudinal coordinate of city | 1950-2013 temperature data from the Berkeley Earth Surface Temperature Study |
| median_avg_temp | double | median average temperature | 1950-2013 temperature data from the Berkeley Earth Surface Temperature Study |
