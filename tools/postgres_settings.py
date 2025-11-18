#!/usr/bin/env python3
import argparse
import sys
import yaml
import psycopg2
import psycopg2.extras

def get_postgres_settings(conn_params):
    """Fetches non-internal settings from a PostgreSQL database."""
    try:
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""
                    SELECT name, short_desc, vartype, unit, min_val, max_val, enumvals, reset_val
                    FROM pg_settings
                    WHERE context <> 'internal'
                """)
                return {row['name']: dict(row) for row in cur.fetchall()}
    except psycopg2.Error as e:
        print(f"Database connection error: {e}", file=sys.stderr)
        sys.exit(1)

def get_yaml_categories(yaml_path):
    """Loads setting categories from the parameters.yaml file."""
    try:
        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)
            if 'categories' in data and isinstance(data['categories'], dict):
                return data['categories']
            else:
                print(f"Error: 'categories' key not found or not a dictionary in {yaml_path}", file=sys.stderr)
                sys.exit(1)
    except FileNotFoundError:
        print(f"Error: YAML file not found at {yaml_path}", file=sys.stderr)
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file: {e}", file=sys.stderr)
        sys.exit(1)

def generate_schema_yaml(db_settings_data, categories):
    """Generates a categorized JSON schema structure for the given settings."""
    schema = {
        'type': 'object',
        'description': 'Schema for PostgreSQL configuration settings (postgresql.conf), grouped by category.',
        'properties': {}
    }

    type_mapping = {
        'bool': 'boolean',
        'integer': 'integer',
        'real': 'number',
        'string': 'string',
        'enum': 'string'
    }

    for category_name, settings_list in categories.items():
        category_schema = {
            'type': 'object',
            'description': f'Settings for the {category_name} category.',
            'properties': {}
        }

        for setting_name in sorted(settings_list):
            setting_data = db_settings_data.get(setting_name)
            if not setting_data:
                print(f"Warning: Setting '{setting_name}' from YAML not found in database data. Skipping.", file=sys.stderr)
                continue

            prop = {}
            pg_type = setting_data['vartype']
            json_type = type_mapping.get(pg_type, 'string')
            prop['type'] = json_type

            description = setting_data['short_desc']
            if setting_data['unit']:
                description += f" (Unit: {setting_data['unit']})"
            prop['description'] = description

            # Add default value, converting type from string as needed
            reset_val = setting_data['reset_val']
            if reset_val is not None:
                try:
                    if pg_type == 'bool':
                        prop['default'] = (reset_val == 'on')
                    elif pg_type == 'integer':
                        prop['default'] = int(reset_val)
                    elif pg_type == 'real':
                        prop['default'] = float(reset_val)
                    else: # string, enum
                        prop['default'] = reset_val
                except (ValueError, TypeError) as e:
                    print(f"Warning: Could not convert reset_val '{reset_val}' for setting '{setting_name}'. Error: {e}", file=sys.stderr)


            if pg_type == 'integer' and setting_data['min_val'] is not None:
                prop['minimum'] = int(setting_data['min_val'])
            if pg_type == 'integer' and setting_data['max_val'] is not None:
                prop['maximum'] = int(setting_data['max_val'])
            elif pg_type == 'real' and setting_data['min_val'] is not None:
                prop['minimum'] = float(setting_data['min_val'])
            if pg_type == 'real' and setting_data['max_val'] is not None:
                prop['maximum'] = float(setting_data['max_val'])

            if pg_type == 'enum' and setting_data['enumvals']:
                prop['enum'] = setting_data['enumvals']

            category_schema['properties'][setting_name] = prop

        schema['properties'][category_name] = category_schema

    return schema

def main():
    """Compares settings and generates a categorized schema."""
    parser = argparse.ArgumentParser(
        description="Compare PostgreSQL settings and generate a categorized schema file."
    )
    parser.add_argument("-d", "--dbname", required=True, help="Database name")
    parser.add_argument("-u", "--user", required=True, help="Database user")
    parser.add_argument("-p", "--password", required=True, help="Database password")
    parser.add_argument("-H", "--host", default="localhost", help="Database host")
    parser.add_argument("-P", "--port", default="5432", help="Database port")
    parser.add_argument(
        "--yaml-file",
        default="source/parameters.yaml",
        help="Path to the parameters.yaml file"
    )
    parser.add_argument(
        "--output-schema-file",
        default="source/postgresql.conf.yaml",
        help="Path to the output schema file."
    )

    args = parser.parse_args()

    conn_params = {
        "dbname": args.dbname,
        "user": args.user,
        "password": args.password,
        "host": args.host,
        "port": args.port,
    }

    print("--- Comparing PostgreSQL settings ---")
    print(f"Connecting to {args.host}:{args.port}...")

    db_settings_data = get_postgres_settings(conn_params)
    db_settings = set(db_settings_data.keys())
    print(f"Found {len(db_settings)} configurable settings in the database.")

    print(f"Loading settings from {args.yaml_file}...")
    yaml_categories = get_yaml_categories(args.yaml_file)
    yaml_settings = {setting for sublist in yaml_categories.values() for setting in sublist}
    print(f"Found {len(yaml_settings)} settings in {len(yaml_categories)} categories in the YAML file.")

    # --- Comparison ---
    in_yaml_not_in_db = sorted(list(yaml_settings - db_settings))
    in_db_not_in_yaml = sorted(list(db_settings - yaml_settings))

    print("\n--- Comparison Results ---")

    if not in_yaml_not_in_db and not in_db_not_in_yaml:
        print("✓ The settings in the YAML file and the database are perfectly in sync.")

        print(f"Generating categorized schema for {args.output_schema_file}...")
        schema_content = generate_schema_yaml(db_settings_data, yaml_categories)
        try:
            with open(args.output_schema_file, 'w') as f:
                yaml.dump(schema_content, f, sort_keys=False, default_flow_style=False, indent=2)
            print(f"✓ Successfully generated {args.output_schema_file}.")
        except IOError as e:
            print(f"Error writing to {args.output_schema_file}: {e}", file=sys.stderr)
            sys.exit(1)

    else:
        if in_yaml_not_in_db:
            print("\n✗ Settings in YAML but NOT in the database (potential typos or old settings):")
            for item in in_yaml_not_in_db:
                print(f"  - {item}")

        if in_db_not_in_yaml:
            print("\n✗ Settings in the database but NOT in YAML (missing from YAML):")
            for item in in_db_not_in_yaml:
                print(f"  - {item}")

        sys.exit(1)

if __name__ == "__main__":
    main()
