def join_targets(sales_df, targets_df):
    return sales_df.join(targets_df, on="region", how="left")
