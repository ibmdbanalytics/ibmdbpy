from nzpyida import IdaDataFrame


def build_result(output_table, merge_output, db, df, output_signature, table_name, query, id):

    if output_table and merge_output is False:
        if db.exists_table(output_table):
            raise NameError("table name already exists..choose a different name")

        create_string = "create table " + output_table + " as "
        query = create_string + query
        result = df.ida_query(query, autocommit=True)
        idadf = IdaDataFrame(db, output_table)
        return idadf
    if output_table and merge_output is True:
        if db.exists_table(output_table):
            raise NameError("table name already exists..choose a different name")

        # check for duplicate columns

        for column in output_signature:



            if column in df.columns.tolist():
                if (column == id):
                    continue

                raise ValueError("column " + column + " duplicated in the output table")


        # create a random table for merging the two tables
        output_table_tmp = db._get_valid_tablename(prefix="pyida_")
        create_string = "create table " + output_table_tmp + " as "
        query = create_string + query
        result = df.ida_query(query, autocommit=True)

        # join the two
        columns_str = ""
        for column in output_signature:

            if column == id:
                continue
            columns_str = columns_str + "link." + column + ","
        #if len(columns_str) > 0:
            #columns_str = columns_str[:-1]

        query = "create table " + output_table + " as  select  " + columns_str + " base.*  from  " + output_table_tmp + " as link INNER JOIN " \
                                                                                                                             " " + table_name + "  as base on link." + id + " = base." + id + ";"


        result = df.ida_query(query, autocommit=True)
        db.drop_table(output_table_tmp)
        idadf = IdaDataFrame(db, output_table)
        return idadf
    if output_table is None and merge_output is True:

        # create a random table and store query results
        output_table_tmp = db._get_valid_tablename(prefix="pyida_")
        create_string = "create table " + output_table_tmp + " as "
        query = create_string + query
        result = df.ida_query(query, autocommit=True)


        # join the two

        # check if there are duplicate columns in the two tables

        for column in output_signature:

            if column in df.columns.tolist():
                if (column == id):
                    continue
                raise ValueError("column "+column+" duplicated in the output table")

        columns_str = ""
        for column in output_signature:
            if column == id:
                continue
            columns_str = columns_str + "link. " + column + ","
        if len(columns_str) > 0:
            columns_str = columns_str[:-1]
        df_output_table = db._get_valid_tablename(prefix="df_")

        query = "create table "+df_output_table+" as  select  " + columns_str + " , base.*  from  " + output_table_tmp + " as link INNER JOIN " \
                                                                                                                        " " + table_name+"  as base on link." + id + " = base." +id + ";"


        result = df.ida_query(query, autocommit=True)


        idadf = IdaDataFrame(db, df_output_table)

        db.drop_table(output_table_tmp)

        return idadf

    df_output_table = db._get_valid_tablename(prefix="df_")
    query = "create table " + df_output_table + " as " + query
    result = df.ida_query(query)

    idadf = IdaDataFrame(db, df_output_table)
    return idadf


def compress_columns_string(columns):


    columns_count = len(columns)

    compressions_count = columns_count // 64

    residual_columns_count = columns_count % 64


    compressions_string = ''
    for i in range(0, compressions_count):
        columns_string = ",".join(columns[(i * 64):(i + 64)])

        compressions_string = compressions_string + "NZAEMULTIARG(" + columns_string + "),  "

    columns_string = ",".join(
        columns[(compressions_count * 64):((compressions_count * 64) + residual_columns_count)])
    compressions_string = compressions_string +columns_string


    return compressions_string
