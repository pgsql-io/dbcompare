# DBCOMPARE

It is a java utility to perform table/data comparison between Oracle and
PostgreSQL.

## Configuration

`dbcompare.conf` file can be used to change settings e.g. Oracle and PostgreSQL
connection information.

## Run

The utility can be executed on unix/linux as following i.e.
	`./dbcompare.sh -s schema1 schema2`

It will create 2 CSV files as `pg.csv` and `ora.csv` in the following format i.e.
	`table_name,row_count,index_count`

These generated files can be compared with diff or similar utilities.

## Licence

(C) 2022 OSCG-PARTNERS. All rights reserved.
