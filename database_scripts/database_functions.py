# Some useful functions to avoid repetition when inserting data into database tables

# inner query returns query object that filters the table using the given condition
# outer query returns query object that checks existence
# .scalar() actually gives the value of True or False
def check_value_exists(SomeMappedClass, some_column, some_value):
    subquery = session.query(SomeMappedClass).filter(SomeMappedClass.some_column == some_value)
    exists = session.query(subquery.exists()).scalar()
    return exists

def get_dataframe_from_s3(bucket, key):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        file = response['Body'].read()
        df = pickle.loads(file)
        return df
    except Exception as e:
        print(f"File {key} may not exist in bucket {bucket} or there is an issue \
            with reading or parsing the contents of the file: {str(e)}")   


# def insert_from_df(DataFrame):
#     data = DataFrame.to_dict('records')
#     session.add_all(data)

