This repository houses python ETL script that extract signal preventive maintainance work order information from Fulcrum to a PostgreSQL based staging database. Data in the staging database will be feed in to Knack an hour after the staging database is updated each day.



## Requirement

[fulcrum](https://github.com/fulcrumapp/fulcrum-python)

[pypgrest](https://pypi.org/project/pypgrest/)



## License

As a work of the City of Austin, this project is in the public domain within the United States.

Additionally, we waive copyright and related rights of the work worldwide through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/).
