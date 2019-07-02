# Project 3: Cloud Data Warehouse

This project aims to create a database that allows a dimensional analysis of the music played on a music streaming platform. This platform belongs to a fictitious star, Sparkify, Data comes from record files and sets of song data.

To provide this analytical data, a dimensional modeling was created in a Redshift. This modeling will be loaded by an ETL process written in Python 3.6 that can be executed on a programming basis.

With this database you can get answers to several questions, among them:
* Visions on the volumetry of songs in the different temporalities
* Frequency of use of the platform by state
* Most Popular Artists and Songs

## DWH configurations
* Set Redshift cluster host
* Set user name in Redshift
* Set password name in Redshift
* Set port on redshift
* Set Redshift ARN

## How to Use

1. Run create_tables.py from terminal.
2. Run etl.py from terminal.

## Test
An interactive test of the ETL process load can be done through the notebook Jupyter test.ipynb