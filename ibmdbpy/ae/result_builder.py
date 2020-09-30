from ibmdbpy import IdaDataFrame


def build_result(output_table, merge_output, db, df, output_signature, table_name, query):
    if output_table and merge_output is False:
        if db.exists_table(output_table):
            create_string = "insert into  " + output_table + " "
        else:
            create_string = "create table " + output_table + " as "
        query = create_string + query
        result = df.ida_query(query, autocommit=True)
        idadf = IdaDataFrame(db, output_table)
        df = idadf.as_dataframe()
        return df
    if output_table and merge_output is True:
        create_string = "create table " + output_table + "_temp as "
        query = create_string + query
        result = df.ida_query(query, autocommit=True)
        # join the two
        columns_str = ""
        for column in output_signature:
            if column == 'ID':
                continue
            columns_str = columns_str + "link. " + column + ","
        if len(columns_str) > 0:
            columns_str = columns_str[:-1]

        print(columns_str)

        if db.exists_table(output_table):
            create_string = "insert into  " + output_table + " "
        else:
            create_string = "create table " + output_table + " as  "

        query = create_string + " select  " + columns_str + " , base.*  from  " + output_table + "_temp as link INNER JOIN  " + table_name + " as base on link.ID = base.ID;"
        result = df.ida_query(query, autocommit=True)
        db.drop_table(output_table + "_temp")
        idadf = IdaDataFrame(db, output_table)
        df = idadf.as_dataframe()
        return df
    result = df.ida_query(query)
    return result
