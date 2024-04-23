from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, DateType, TimestampType, BooleanType, DecimalType
 
def get_field_type(field_name):
    """Determines the data type of a field based on its name."""
    if field_name.endswith('$numberDecimal'):
        return DecimalType(18,3)
    elif field_name.endswith('$numberLong'):
        return LongType()
    elif field_name.endswith('$date'):
        return LongType()
    elif field_name.endswith('$oid'):
        return StringType()
    elif field_name.endswith('$boolean'):
        return BooleanType()
    elif field_name.endswith('$timestamp'):
        return TimestampType()
    else:
        return StringType()
 
def field_exists(df, field_path):
    """Checks if a field exists in the DataFrame."""
    try:
        return (field_path in df.schema.fieldNames())
    except Exception:
        return False
 
def add_struct_field(df, struct_path, field_name, field_type):
    """Adds a new field to a struct in the DataFrame."""
    struct_parts = struct_path.split('.')
    if len(struct_parts) == 1:
        struct_name = struct_parts[0]
        if struct_name in df.columns:
            if isinstance(df.schema[struct_name].dataType, StructType):
                existing_fields = df.schema[struct_name].dataType.fields
                new_struct_fields = [col(f"{struct_name}.{field.name}").alias(field.name) for field in existing_fields]
                if not any(field.name == field_name for field in existing_fields):
                    new_struct_fields.append(lit(None).cast(field_type).alias(field_name))
                new_struct = struct(*new_struct_fields)
            else:
                new_struct = struct(lit(None).cast(field_type).alias(field_name))
            return df.withColumn(struct_name, new_struct)
        else:
            new_struct = struct(lit(None).cast(field_type).alias(field_name))
            return df.withColumn(struct_name, new_struct)
    else:
        inner_struct_name = struct_parts[-1]
        inner_struct_path = '.'.join(struct_parts[:-1])
        df = add_struct_field(df, inner_struct_path, inner_struct_name, StructType([StructField(field_name, field_type, True)]))
        return df
 
def add_struct_fields_recursive(df, field_path, field_type):
    """Adds fields recursively to a DataFrame."""
    field_parts = field_path.split('.')
    if len(field_parts) == 1:
        return add_struct_field(df, field_parts[0], field_parts[0], field_type)
    else:
        return add_struct_field(df, '.'.join(field_parts[:-1]), field_parts[-1], field_type)
 
def unifySchema(df: DataFrame, columns: list) -> DataFrame:
    """Adds missing columns and structs to the DataFrame based on a given schema."""
    try:
        for col_name in columns:
            if (not field_exists(df, col_name)) or ( col_name in df.columns and df.select(col(col_name)).filter(col(col_name).isNotNull()).count() == 0):
                if '.' in col_name:
                    field_parts = col_name.split('.')
                    field_type = get_field_type(field_parts[-1])
                    df = add_struct_fields_recursive(df, col_name, field_type)
                    
                else:
                    df = df.withColumn(col_name, lit(None).cast(StringType()))
                    
    except Exception as e:
        print(f'error in {col_name} : ')
        raise(e)
 
    return df
