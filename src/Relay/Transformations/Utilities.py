
def update_dataframe_column(dataframe, column_name, new_column_suffix, value, default_value):

    # Determine the name of the new column
    new_column_name = "{0}{1}".format(column_name, new_column_suffix)

    # Add the new column to the dataframe
    if new_column_name not in dataframe.columns:
        dataframe[new_column_name] = default_value

    n = dataframe.shape[0]

    if n == 0:
        dataframe.at[0, new_column_name] = value
    else:
        dataframe.at[n - 1, new_column_name] = value
