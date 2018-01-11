"""Project: Eskapade - A python-based package for data analysis.

Module: spark_analysis.data_conversion

Created: 2017/05/30

Description:
    Converters between Spark, Pandas, and Python data formats

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import uuid

import pyspark

from eskapade.helpers import apply_transform_funcs
from eskapade.logger import Logger

SPARK_SQL_TYPES = pyspark.sql.types._type_mappings
logger = Logger()


def create_spark_df(spark, data, schema=None, process_methods=None, **kwargs):
    """Create a Spark data frame from data in a different format.

    A Spark data frame is created with either a specified schema or a schema
    inferred from the input data.  The schema can be specified with the
    keyword argument "schema".

    Functions to transform the data frame after creation can be specified by
    the keyword argument "process_methods".  The value of this argument is
    an iterable of (function, arguments, keyword arguments) tuples to apply.

    The data frame is created with the createDataFrame function of the
    SparkSession.  Remaining keyword arguments are passed to this function.

    >>> spark = pyspark.sql.SparkSession.builder.getOrCreate()
    >>> df = create_spark_df(spark,
    >>>                      [[1, 1.1, 'one'], [2, 2.2, 'two']],
    >>>                      schema=['int', 'float', 'str'],
    >>>                      process_methods=[('repartition', (), {'numPartitions': 6})])
    >>> df.show()
    +---+-----+---+
    |int|float|str|
    +---+-----+---+
    |  2|  2.2|two|
    |  1|  1.1|one|
    +---+-----+---+

    :param pyspark.sql.SparkSession spark: SparkSession instance
    :param data: input dataset
    :param schema: schema of created data frame
    :param iterable process_methods: methods to apply on the data frame after creation
    :returns: created data frame
    :rtype: pyspark.sql.DataFrame
    """
    # check if data-frame schema was provided
    if isinstance(schema, int):
        # infer schema from a single row (prevents Spark >= 1.6.1 from checking schema of all rows)
        def get_row(data, ind):
            """Get row."""
            try:
                return data.iloc[ind].tolist()
            except AttributeError:
                pass
            try:
                row = data.first()
                if ind > 0:
                    logger.warning('Inferring data-frame schema from first row, instead of row with index {i:d}', i=ind)
                return row
            except AttributeError:
                pass
            try:
                return data[ind]
            except TypeError:
                raise TypeError('Unable to get row from data of type "{!s}" to infer schema.'.format(type(data)))

        row = get_row(data, schema)

        def to_python_type(var):
            """Get item."""
            try:
                return var.item()
            except AttributeError:
                return var

        schema = pyspark.sql.types._infer_schema(tuple(to_python_type(it) for it in row))
        try:
            for t, n in zip(schema.fields, data.columns):
                t.name = str(n)
        except AttributeError:
            pass
    elif isinstance(schema, dict):
        # create schema from dictionary of (name, data type) pairs
        schema = df_schema(schema)
    kwargs['schema'] = schema

    # check if input is a data frame
    if isinstance(data, pyspark.sql.DataFrame):
        if not kwargs['schema']:
            kwargs['schema'] = data.schema
        data = data.rdd

    # create and transform data frame
    df = spark.createDataFrame(data, **kwargs)
    if process_methods:
        df = apply_transform_funcs(df, process_methods)
    return df


def df_schema(schema_spec):
    """Create Spark data-frame schema.

    Create a schema for a Spark data frame from a dictionary of (name, data
    type) pairs, describing the columns.  Data types are specified by Python
    types or by Spark-SQL types from the pyspark.sql.types module.

    >>> from collections import OrderedDict as odict
    >>> schema_dict = odict()
    >>> schema_dict['foo'] = pyspark.sql.types.IntegerType()
    >>> schema_dict['bar'] = odict([('descr', str), ('val', float)])
    >>> print(schema_dict)
    OrderedDict([('foo', IntegerType), ('bar', OrderedDict([('descr', <class 'str'>), ('val', <class 'float'>)]))])
    >>> spark = pyspark.sql.SparkSession.builder.getOrCreate()
    >>> df = spark.createDataFrame([(1, ('one', 1.1)), (2, ('two', 2.2))], schema=df_schema(schema_dict))
    >>> df.show()
    +---+---------+
    |foo|      bar|
    +---+---------+
    |  1|[one,1.1]|
    |  2|[two,2.2]|
    +---+---------+

    :param dict schema_spec: schema specification
    :returns: data-frame schema
    :rtype: pyspark.sql.types.StructType
    :raises: TypeError if data type is specified incorrectly
    """
    def get_field(name, data_type):
        """Return a struct field for specified data type."""
        # treat dictionaries as struct types
        if isinstance(data_type, dict):
            data_type = pyspark.sql.types.StructType([get_field(*spec) for spec in data_type.items()])

        # convert Python types to Spark-SQL types
        data_type = SPARK_SQL_TYPES.get(data_type, data_type)

        # convert Spark-SQL type classes to Spark-SQL types
        if isinstance(data_type, type) and issubclass(data_type, pyspark.sql.types.DataType):
            data_type = data_type()

        # check and return data type
        if not isinstance(data_type, pyspark.sql.types.DataType):
            raise TypeError('Type specifications for data-frame schemas must be DataTypes or dictionaries')
        return pyspark.sql.types.StructField(str(name), data_type)

    # return a struct type with a list of struct fields for specified data types
    return pyspark.sql.types.StructType([get_field(*spec) for spec in schema_spec.items()])


def hive_table_from_df(spark, df, db, table):
    """Create a Hive table from a Spark data frame.

    :param pyspark.sql.SparkSession spark: SparkSession instance
    :param pyspark.sql.DataFrame df: input data frame
    :param str db: database for table
    :param str table: name of table
    """
    # register temporary table
    temp_name = '{0:s}_{1:s}'.format(table, uuid.uuid4().hex)
    df.createOrReplaceTempView(temp_name)

    # create table
    table_spec = '.'.join(s for s in (db, table) if s)
    create_table_query = 'CREATE TABLE {spec} AS SELECT {cols} FROM {name}'\
        .format(name=temp_name, spec=table_spec, cols=', '.join(c for c in df.columns))
    logger.debug(create_table_query)
    spark.sql(create_table_query)
