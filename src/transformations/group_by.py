def group_by_sales(df):
    return df.groupBy("region").sum("sales")
