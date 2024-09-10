import sys
import psycopg2
from db_config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_SCHEMA

def connect_to_db():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)

def execute_query(query):
    try:
        cursor.execute(query)
        return cursor.fetchall()
    except (Exception, psycopg2.Error) as error:
        print(f"Error executing query: {error}")
        close_resources()
        quit()

def check_no_non_key_columns(columns):
    columns_length = len(columns)
    if (columns_length == 0):
        return True
def is_table_empty(table):
    query = f"""SELECT COUNT(*) 
                FROM {table}"""
    result = execute_query(query)
    return result[0][0] == 0

def close_resources():
    if cursor:
        cursor.close()
    if connection:
        connection.close()

def test_minimality(table, pks, columns):
    for pk in pks:
        counter = 0
        for column in columns:
            query = f"""
                        SELECT * 
                        FROM (
                              SELECT {pk}, COUNT(DISTINCT {column}) AS cnt
                              FROM {table}
                              GROUP BY {pk}
                              ) AS t1
                        WHERE t1.cnt > 1
                    """
            record_sql(query)
            result = execute_query(query)

            if len(result) == 0:
                counter += 1

            if counter == len(columns):
                return False

    return True

def test_primary_key(table, *pks):
    query = f"""SELECT * 
                    FROM (
                            SELECT {pk}, COUNT(*) AS cnt 
                            FROM {table} 
                            GROUP BY {pk}
                        ) t1 
                    WHERE t1.cnt > 1
                """
    record_sql(query)
    result = execute_query(query)

    return len(result) == 0

def test_first_normal_form(table, pks, columns):
    keys = ", ".join(pks)

    query = f"""SELECT {keys}, {columns}, COUNT(*) as cnt 
                FROM {table} 
                GROUP BY {keys},{columns} 
                HAVING COUNT(*) > 1
             """
    record_sql(query)
    result = execute_query(query)
    return len(result) == 0

def test_second_normal_form(table, pks, columns):
    for pk in pks:
        for column in columns:
            query = f"""
                        SELECT * 
                        FROM (
                              SELECT {pk}, COUNT(DISTINCT {column}) AS cnt
                              FROM {table}
                              GROUP BY {pk}
                              ) AS t1
                        WHERE t1.cnt > 1
            """
            record_sql(query)
            result = execute_query(query)
            if len(result) == 0:
                return False

    return True

def test_third_normal_form(table, columns):
    columns_length = len(columns)
    # if we have only 1 non-key column, we're automatically in third normal form.
    if (columns_length == 1):
        return True

    # if we have only 2 non-key columns, we can handle the comparison right here.
    if (columns_length == 2):
        query = f"""
                        SELECT * 
                            FROM (
                                  SELECT {columns[0]}, COUNT(DISTINCT {columns[1]}) AS cnt
                                  FROM {table}
                                  GROUP BY {columns[0]}
                                  ) AS t1
                            WHERE t1.cnt > 1
                    """
        record_sql(query)
        result = execute_query(query)
        if len(result) == 0:
            return False

        query = f"""
                        SELECT * 
                            FROM (
                                   SELECT {columns[1]}, COUNT(DISTINCT {columns[0]}) AS cnt
                                   FROM {table}
                                   GROUP BY {columns[1]}
                                  ) AS t1
                            WHERE t1.cnt > 1
                 """
        record_sql(query)
        result = execute_query(query)
        if len(result) == 0:
            return False

        return True


    for column in range(columns_length):
        for i in range(column + 1, columns_length):
            query = f"""
                        SELECT * 
                            FROM (
                                  SELECT {column}, COUNT(DISTINCT {columns[i]}) AS cnt
                                  FROM {table}
                                  GROUP BY {columns[0]}
                                  ) AS t1
                            WHERE t1.cnt > 1
                        """
            record_sql(query)
            result = execute_query(query)
            if len(result) == 0:
                return False

    # Then we reverse the list of columns, so that we can get the other half of the cartesian product.

    reversed_columns = columns[::-1]

    # For example, this gives us (COL 2, COL 1), (COL 3, COL 1).... (COL 3, COL 2)
    for column in range(columns_length):
        for i in range(column + 1, columns_length):
            query = f"""
                        SELECT * 
                            FROM (
                                  SELECT {column}, COUNT(DISTINCT {reversed_columns[i]}) AS cnt
                                  FROM {table}
                                  GROUP BY {columns[0]}
                                  ) AS t1
                            WHERE t1.cnt > 1
                        """
            record_sql(query)
            result = execute_query(query)
            if len(result) == 0:
                return False

    return True




def test_BCNF(table, pks, columns):
    for pk in pks:
        for column in columns:
            #This is effectively the reverse of the 2NF, where we check to make sure that none of our non-key columns generate
            #any key columns.
            query = f"""
                        SELECT * 
                        FROM (
                              SELECT {column}, COUNT(DISTINCT {pk}) AS cnt
                              FROM {table}
                              GROUP BY {column}
                              ) AS t1
                        WHERE t1.cnt > 1
            """
            record_sql(query)
            result = execute_query(query)
            if len(result) == 0:
                return False

    return True
def output_to_txt(txt):
    with open('rnf.txt', 'a') as fout:
        fout.write(txt + '\n')

def record_sql(sql_query):
    with open('rnf.sql', 'a') as sql_file:
        sql_file.write(sql_query + '\n')

def output_to_sql(table_name):
    with open('rnf.sql', 'a') as sql_file:
        sql_file.write(f'-- Table Name: {table_name}\n\n')

if __name__ == "__main__":
    connection, cursor = None, None
    output = ""

    try:
        # parses db
        connection = connect_to_db()
        cursor = connection.cursor()
        args = sys.argv[1].split(";")
        table, pk, columns = [arg.split("=")[1] for arg in args]

        table_with_schema = f"{DB_SCHEMA}.{table}"
        pks = pk.split(",")
        columns_arr = columns.split(",")

        output += table + "\n"
        output_to_sql(table)
        if is_table_empty(table_with_schema) or check_no_non_key_columns(columns):
            output += "PK valid\n"
            output += "1NF Y\n"
            output += "2NF Y\n"
            output += "3NF Y\n"
            output += "BCNF Y"
        else:
            if len(pks) == 1:
                pk_valid = test_primary_key(table_with_schema, *pks)

                output += "PK " + ("valid" if pk_valid else "invalid") + "\n"
                output += "1NF " + ("Y" if test_first_normal_form(table_with_schema, pks, columns) else "N") + "\n"

                # If we have an invalid pk, we can't have 2NF, 3NF, or BNCF--so we check if we have an invalid pk and output "N"
                if (pk_valid == False):
                    output += "2NF " + "N" + "\n"
                    output += "3NF " + "N" + "\n"
                    output += "BCNF " + "N"
                else:
                    # We don't have to run 2NF test in this case, since we have 1 pk, all we do then is test 1NF
                    output += "2NF " + ("Y" if test_first_normal_form(table_with_schema, pks, columns) else "N") + "\n"
                    output += "3NF " + (
                        "Y" if test_third_normal_form(table_with_schema, columns_arr) and test_first_normal_form(table_with_schema, pks,
                                                                                                     columns) else "N") + "\n"
                    output += "BCNF " + ("Y" if test_third_normal_form(table_with_schema, columns_arr) else "N")

            # If composite key
            else:
                pk_valid = test_primary_key(table_with_schema,*pks)
                output += "PK " + ("valid" if pk_valid else "invalid") + "\n"
                output += "1NF " + ("Y" if test_first_normal_form(table_with_schema, pks, columns) else "N") + "\n"

                if (pk_valid == False):
                    output += "2NF " + "N" + "\n"
                    output += "3NF " + "N" + "\n"
                    output += "BCNF " + "N"
                else:
                    output += "2NF " + (
                        "Y" if test_second_normal_form(table_with_schema, pks, columns_arr) and test_first_normal_form(table_with_schema, pks,
                                                                                                           columns) else "N") + "\n"
                    output += "3NF " + (
                        "Y" if test_third_normal_form(table_with_schema, columns_arr) and test_second_normal_form(table_with_schema, pks,
                                                                                                      columns_arr) else "N") + "\n"

                    output += "BCNF " + (
                        "Y" if test_BCNF(table_with_schema, pks, columns_arr)
                               and test_second_normal_form(table_with_schema, pks, columns_arr)
                               and test_third_normal_form(table_with_schema, columns_arr)
                               and test_first_normal_form(table_with_schema, pks, columns) else "N"
                    )


    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        close_resources()

        # Print the output to the console
    print(output)

    # Then write the output to the file
    output_to_txt(output)