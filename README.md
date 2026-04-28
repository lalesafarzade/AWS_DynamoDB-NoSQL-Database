# DynamoDB Manager CLI

A simple Python command-line tool to manage AWS DynamoDB tables and data.

---

## Features

- Create tables from JSON configuration
- List and delete tables
- Load data (single item or batch)
- Scan and query tables
- Get, update, and delete items
- Transaction write support

---

## Project Structure


.
├── main.py # Core DynamoDBManager class
├── app.py # CLI interface
├── config.json # Example config file
└── README.md


---

## Prerequisites

- Python 3.8+
- AWS account
- AWS CLI configured

Install dependency:


pip install boto3


Configure AWS:


aws configure


---

## Usage

Run commands using:


python app.py <command> [arguments]


---

## Commands

### Create Tables


python app.py create-tables config.json


### List Tables


python app.py list-tables


### Delete Table


python app.py delete-table <table_name>


### Load Data (Single Item)


python app.py load-data config.json


### Load Batch Data


python app.py load-batch config.json


### Scan Table


python app.py scan-table <table_name>


### Get Item


python app.py get-item <table_name> '{"id": {"S": "123"}}'


### Query Table


python app.py query-table <table_name> '{"KeyConditionExpression": "..."}'


### Update Item


python app.py update-item <table_name> '{"id": {"S": "123"}}' '{"UpdateExpression": "..."}'


### Delete Item


python app.py delete-item <table_name> '{"id": {"S": "123"}}'


### Transaction Write


python app.py transact-write '[{...}]' '{}'