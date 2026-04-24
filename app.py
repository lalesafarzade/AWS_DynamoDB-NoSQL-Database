from main import  DynamoDBManager
import argparse

def main():
    parser = argparse.ArgumentParser(description="dynamo")

    subparsers = parser.add_subparsers(dest="dynamo_command")



# create tables from JSON
    create_table = subparsers.add_parser("create-tables")
    create_table.add_argument("json_file")

    # list tables
    list_tables = subparsers.add_parser("list-tables")

    # delete table
    delete_table = subparsers.add_parser("delete-table")
    delete_table.add_argument("table_name")

    #load data item by item
    load_data = subparsers.add_parser("load-data")
    load_data.add_argument("config_file")

    #load batch data 
    load_batch= subparsers.add_parser("load-batch")
    load_batch.add_argument("config_file")


    args = parser.parse_args()
    dynamo_manager = DynamoDBManager()

    if args.dynamo_command == "create-tables":
        dynamo_manager.create_tables_from_json(args.json_file)

    elif args.dynamo_command == "list-tables":
        dynamo_manager.list_tables()

    elif args.dynamo_command == "delete-table":
        dynamo_manager.delete_table(args.table_name)   
        
    elif args.dynamo_command == "load-data":
        dynamo_manager.load_data_from_json(args.config_file)

    elif args.dynamo_command == "load-batch":
        dynamo_manager.batch_write_item_db(args.config_file)


if __name__ == "__main__":
    main()