# Databricks notebook source
def load_config(config_file_path):
    try:
        with open(config_file_path, 'r') as file:
            config_yml = yaml.safe_load(file)
        return config_yml
    except Exception as ex:
        msg = f"loading config {config_file_path} failed with error:{ex}"
        print(msg)

def map_yaml_to_pyspark_type(yaml_type):
    type_mapping = {
        "string": StringType(),
        "integer": IntegerType(),
        "double": DoubleType(),
        "boolean": BooleanType(),
        "struct": StructType,
    }
    return type_mapping.get(yaml_type.lower(), StringType())

def build_pyspark_schema(schema_dict):
    schema_type = schema_dict['type'].lower()
    
    if schema_type == 'struct':
        fields = []
        for field in schema_dict.get('fields', []):
            field_name = field['name']
            field_type = field['type']
            
            if field_type.lower() == 'struct':
                struct_fields = build_pyspark_schema(field)
                fields.append(StructField(field_name, struct_fields, field.get('nullable', True)))
            elif field_type.lower() == 'array':
                element_type = field['elementType']
                if isinstance(element_type, dict) and element_type['type'].lower() == 'struct':
                    array_element_type = build_pyspark_schema(element_type)
                else:
                    array_element_type = map_yaml_to_pyspark_type(element_type)
                fields.append(StructField(field_name, ArrayType(array_element_type, containsNull=field.get('containsNull', True)), field.get('nullable', True)))
            else:
                pyspark_type = map_yaml_to_pyspark_type(field_type)
                fields.append(StructField(field_name, pyspark_type, field.get('nullable', True)))
        
        return StructType(fields)
    
    elif schema_type == 'array':
        element_type = schema_dict['elementType']
        if isinstance(element_type, dict) and element_type['type'].lower() == 'struct':
            array_element_type = build_pyspark_schema(element_type)
        else:
            array_element_type = map_yaml_to_pyspark_type(element_type)
        return ArrayType(array_element_type, containsNull=schema_dict.get('containsNull', True))
    
    else:
        return map_yaml_to_pyspark_type(schema_type)
    
def get_schema_for_action(record_type, action_type):
    schema_full_path = f"{config_path}/{layer}/{record_type.lower()}_schema.yml"
    try:
        yaml_schema = load_config(schema_full_path)
        for actionType, actionData in yaml_schema['actionType'].items():
            if action_type == actionType:
                json_schema = build_pyspark_schema(actionData['schema'])
                return json_schema     
    except Exception as ex:
        print(f"getting schema for record type {record_type} failed with error:{ex}")
        raise ex

def flatten_json(src_df, col_name_prefix: bool=True):
    flat_cols = []
    cols_to_explode = []
    try:
        for column_name, column_type in src_df.dtypes:
            if column_type.startswith("struct"):
                # Flatten struct columns
                for field in src_df.schema[column_name].dataType.fields:
                    if col_name_prefix:
                        flat_cols.append(col(f"{column_name}.{field.name}").alias(f"{column_name}_{field.name.replace('(','').replace(')','')}"))
                    else:
                        flat_cols.append(col(f"{column_name}.{field.name}").alias(f"{field.name.replace('(','').replace(')','')}"))
            elif column_type.startswith("array"):
                cols_to_explode.append(column_name)
                flat_cols.append(column_name)
            else:
                flat_cols.append(col(column_name))

        flatten_df = src_df.select(flat_cols)

        for col_name in cols_to_explode:
            flatten_df = flatten_df.withColumn(col_name, explode_outer(col(col_name)))
        
        while any(column_type.startswith("struct") or column_type.startswith("array") for _, column_type in flatten_df.dtypes):
            flatten_df = flatten_json(flatten_df)

        return flatten_df

    except Exception as ex:
        msg = "flattening failed with error: {ex}"
        print(msg)

def get_quality_checks(table_name):
    quality_checks= {}
    try:
        config_file_path = f"{config_path}/{layer}/{quality_check_file}"
        config = load_config(config_file_path)
        table_config = config['layer'][layer]["tables"]
        for check in table_config.get(table_name,[]):
            if check['type'] == 'not_null':
                quality_checks[f"{check['field']}_nullChk"] = f"({check['field']} IS NOT NULL AND !({check['field']}=''))"
    except Exception as ex:
        err_msg = f"loading quality checks for {table_name} failed with error:{ex}"
        raise Exception(err_msg)

    return quality_checks

def get_quarantine_rules(table_name):
    quantantine_rules = {}
    quality_rules = get_quality_checks(table_name)

    quantantine_rules["invalid_data"] = f"NOT({' AND '.join(quality_rules.values())})"

    return quantantine_rules