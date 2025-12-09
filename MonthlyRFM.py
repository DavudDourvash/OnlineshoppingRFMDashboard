#%%
# Read Data,Import Packages,Set Parameters & Wrangling
#%%
# [1]
import pandas as pd
from bson import ObjectId
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone
import numpy as np
from tifffile.tifffile import rational
#%%
# [2]
# for Querying MongoDB
beginning_date = datetime(2024, 9, 21, 20, 30)
finishing_date = datetime.now()
module_name = "Onlineshopping"
#%%
df_orders = pd.read_parquet('df_orders.parquet')
df_min_order_date = pd.read_parquet('df_min_order_date.parquet')
df_dim_date = pd.read_parquet("dimdate5.parquet") # to attach shamsi date
rfm_labels = pd.read_excel("OnlineShoppingLabelsUpdate.xlsx", engine='openpyxl') # RFM Label for any modules
#%%
# Set Parameters Before Run --- old version
start_train_date_filter = pd.to_datetime('2024-09-22')
window_table_in_months = 3
# Define cluster categories
low_valued_clusters_labelFa = ['مشتریان جدید', 'مشتریان در معرض خطر', 'مشتریان در معرض خطر ریزش', 'مشتریان کم ارزش']
high_valued_clusters_labelFa = ['مشتریان ارزشمند', 'مشتریان ارزشمند در معرض خطر ریزش', 'مشتریان ارزشمند در معرض ریزش',
                                'مشتریان پتانسیل ارزشمند', 'مشتریان پتانسیل ارزشمند در معرض ریزش', 'مشتریان جدید با پتانسیل رشد',
                                'مشتریان نهنگ های خاموش']
#%%
df_min_order_date = \
    (df_min_order_date
     .assign(date=lambda _df: _df["created_at"] + pd.Timedelta(hours=3, minutes=30))
     .groupby(["user_id"]).agg(
        firstdate=("date", "min")).reset_index()
     .assign(firstdate=lambda _df: _df['firstdate'].dt.normalize())
     .assign(user_id=lambda _df: _df['user_id'].astype('str'))
     )
#%%
# add ShamsiDate to dim_date_join
df_dim_date_join = (
    df_dim_date
    .assign(miladi_d=lambda df: pd.to_datetime(df['miladi_d'], errors='coerce'))
    [['miladi_d', 'jalali_1_s']]
    .assign(
        ShamsiDate=lambda df: pd.to_numeric(
            df['jalali_1_s'].astype(str).str.replace('/', ''),
            errors='coerce'
        )
    )
    .rename(columns={'miladi_d': 'date'})
    .drop(columns=['jalali_1_s'])
    [['date', 'ShamsiDate']]
    .assign(date=lambda df: pd.to_datetime(df["date"]).dt.normalize())
)
#%%
# add shamsifirstdate to df_min_date
df_min_order_date_clean = \
    (df_min_order_date
     .assign(firstdate=lambda df: pd.to_datetime(df['firstdate']).dt.normalize())
     .merge(df_dim_date_join, left_on="firstdate", right_on="date", how='left')
     .rename(columns={'ShamsiDate': 'shamsifirstdate'})
     .drop(columns=['date'])
     )
#%%
# add ShamsiMonth column to df_raw_data
ShamsiMonthRef = [int(f"{year}{month:02}") for year in range(1401, 1410) for month in range(1, 13)]
#%%
df_orders = \
    (df_orders
     .assign(date=lambda df: pd.to_datetime(df['date'], errors='coerce'))
     .rename(columns={"_id": "factor_id"})
     .assign(factor_id=lambda df: df['factor_id'].apply(lambda x: str(x) if isinstance(x, ObjectId) else x))
     .drop(columns=["module"])
     .loc[lambda df: df['date'] >= start_train_date_filter]
     .merge(df_dim_date_join, on="date", how="left")
     .assign(ShamsiMonth=lambda df: df['ShamsiDate'].astype(str).str[:6].astype(int))
     )
min_shamsi_month = df_orders['ShamsiMonth'].min()
#%%
if int(f"{min_shamsi_month}01") not in df_orders['ShamsiDate'].values:
    idx = ShamsiMonthRef.index(min_shamsi_month)
    min_shamsi_month = ShamsiMonthRef[idx + 1]

max_shamsi_month = df_orders['ShamsiMonth'].max()
idx = ShamsiMonthRef.index(max_shamsi_month)
max_shamsi_month = ShamsiMonthRef[idx - 1]

df_orders = (
    df_orders
    .loc[lambda df: df["ShamsiMonth"] >= min_shamsi_month]
    .loc[lambda df: df["ShamsiMonth"] <= max_shamsi_month]
    .merge(df_min_order_date_clean, on="user_id", how="left")
)
#%%
df_orders_with_period = (
    pd.concat(
        [
            df_orders.loc[df_orders["ShamsiMonth"].isin(
                ShamsiMonthRef[idx: idx + window_table_in_months])].assign(
                period=(0 if idx == ShamsiMonthRef.index(min_shamsi_month) else idx - ShamsiMonthRef.index(
                    min_shamsi_month))
            )
            for idx in range(ShamsiMonthRef.index(min_shamsi_month),
                             len(ShamsiMonthRef) - window_table_in_months + 1)
            if (
            (lambda wd: len(wd) > 0 and wd["ShamsiMonth"].nunique() == window_table_in_months)(
                df_orders.loc[
                    df_orders["ShamsiMonth"].isin(ShamsiMonthRef[idx: idx + window_table_in_months])]
            )
        )
        ],
        ignore_index=True
    )
)
#%%
# calculate average days between factors index
df_orders_with_period = (
    df_orders_with_period
    .sort_values(by=["user_id", "period", "date"])
    .assign(
        days_diff=lambda df: df.groupby(["user_id", "period"])["date"].diff().dt.days
    )
    .assign(
        avg_days_between_factors=lambda df: df.groupby(["user_id", "period"])["days_diff"].transform("mean")
    )
)
#%%
# add NewUserLabelAcc to df_raw_data_with_period for new users but new users labeled
period_ranges = (
    df_orders_with_period
    .groupby("period")["ShamsiDate"]
    .agg(["min", "max"])
    .reset_index()
    .rename({"min": "min_date", "max": "max_date"})
)
#%%
df_orders_with_period = (
    df_orders_with_period
    .merge(
        df_orders_with_period
        .groupby("period")["ShamsiDate"]
        .agg(["min", "max"])
        .reset_index()
        .rename(columns={"min": "min_date", "max": "max_date"}),
        on="period", how="left"
    )
    .assign(
        NewUserLabelAcc=lambda df: (
                (df["shamsifirstdate"] >= df["min_date"]) &
                (df["shamsifirstdate"] <= df["max_date"])
        ).astype(int)
    )
)

#%% md
# # RFM Calculation
#%%
# Function to calculate RFM for each monthly period
def calc_rfm_monthly(period, df_orders):
    data_window = df_orders[df_orders["period"] == period].copy()

    if data_window.empty:
        return pd.DataFrame()

    grouped = (
        data_window.groupby("user_id", as_index=False)
        .agg(
            max_date=('date', 'max'),
            F=('factor_id', pd.Series.nunique),
            M=('total_payment_price', 'sum')
        )
    )

    end_date = data_window["date"].max()
    grouped["R"] = (end_date - grouped["max_date"]).dt.days + 1

    # grouped["R_Score_raw"] = pd.qcut(grouped["R"], q=5, labels=False, duplicates='drop') + 1

    grouped['R_Score_raw'] = pd.cut(
    grouped['R'],
    bins=[0, 30, 60, 90, 120, float('inf')],
    labels=[1, 2, 3, 4, 5],
    right=True
    ).astype(int)

    grouped["R_Score"] = 6 - grouped["R_Score_raw"]
    grouped["F_Score"] = pd.qcut(grouped["F"], q=10, labels=False, duplicates='drop') + 1
    grouped["M_Score"] = pd.qcut(grouped["M"], q=5, labels=False, duplicates='drop') + 1

    grouped["period"] = period
    grouped["start_month"] = data_window["ShamsiMonth"].min()
    grouped["end_month"] = data_window["ShamsiMonth"].max()

    grouped = grouped.drop(columns=["R_Score_raw"])

    return grouped
#%%
# calc df_raw_data_with_period_NULAcc column
df_orders_with_period_NULAcc = (
    df_orders_with_period
    [['user_id', 'period', 'shamsifirstdate', 'NewUserLabelAcc']]
    .drop_duplicates()
)
#%%
# Calculate init score_table dataframe
df_score_table = (
    pd.concat(
        [calc_rfm_monthly(p, df_orders_with_period)
         for p in sorted(df_orders_with_period["period"].unique())],
        ignore_index=True
    )
    .assign(
        RFMSeries=lambda df: (
                df["R_Score"].astype(str) +
                df["F_Score"].astype(str) +
                df["M_Score"].astype(str)
        ).astype(int)
    )
    .pipe(lambda df: df.merge(rfm_labels, on="RFMSeries", how="left"))
    .merge(df_orders_with_period_NULAcc, on=["user_id", "period"], how="left")
    .assign(
        clusters_labelFa=lambda df: np.where(
            (df["clusters_labelFa"].isin(["مشتریان جدید", "مشتریان جدید با پتانسیل رشد"])) &
            (df["NewUserLabelAcc"] == 0),
            "مشتریان بازگشته",
            df["clusters_labelFa"]
        )
    )
)
#%% md
# # Add Previous Period Cluster Labels
#%%
# Define rfm_clusters
df_rfm_clusters = df_score_table[["user_id", "period", "RFMSeries", "clusters_labelFa", "clusters_inds"]].copy()
#%%
df_rfm_clusters_prev = (
    df_score_table[["user_id", "period", "RFMSeries", "clusters_labelFa", "clusters_inds"]]
    .assign(period=lambda df: df["period"] + 1)
    .rename(columns={"clusters_labelFa": "prev_clusters_labelFa"})
)
#%%
# add previous clusters label to current
df_rfm_clusters = (df_rfm_clusters
                .merge(df_rfm_clusters_prev[["user_id", "period", "prev_clusters_labelFa"]],
                       on=["user_id", "period"],
                       how="left")
                )
#%%
df_rfm_table = df_score_table[
    ["user_id", "R", "F", "M", "period", "clusters_labelFa", "clusters_inds", "RFMSeries"]].copy()

# ✅ Add prev_clusters_labelFa to rfm_table too
df_rfm_table = df_rfm_table.merge(
    df_rfm_clusters[["user_id", "period", "prev_clusters_labelFa"]],
    on=["user_id", "period"],
    how="left"
)

# calc dist, speed , acceleration based on R, F, M scores
df_cluster_means = (
    df_score_table
    .query('clusters_labelFa != "مشتریان بازگشته"')
    .groupby(["period", "clusters_labelFa"]) # to avoid distortion of means by new customers
    .agg(R_mean=("R_Score", "mean"),
         F_mean=("F_Score", "mean"),
         M_mean=("M_Score", "mean"))
    .reset_index()
)
#%% md
# # Add dist, speed, acceleration and their ranks
#%%
# 1) expand to long form (each user vs each possible target cluster in same period)
df_score_with_targets = (
    df_score_table
    .merge(
        df_cluster_means.rename(columns={"clusters_labelFa": "target_clusters_labelFa"}),
        on="period",
        how="left"
    )
)

df_score_with_targets = df_score_with_targets.assign(
    dist=lambda df: (
                            (df["R_Score"] - df["R_mean"]) ** 2
                            + (df["F_Score"] - df["F_mean"]) ** 2
                            + (df["M_Score"] - df["M_mean"]) ** 2
                    ) ** 0.5
)

df_score_with_targets = df_score_with_targets.sort_values(
    ["user_id", "target_clusters_labelFa", "period"]
)

df_score_with_targets = df_score_with_targets.assign(
    speed=lambda df: df.groupby(
        ["user_id", "target_clusters_labelFa"]
    )["dist"].diff(),
    acc=lambda df: df.groupby(
        ["user_id", "target_clusters_labelFa"]
    )["speed"].diff()
)
#%%
df_score_with_targets_temp = df_score_with_targets.copy()

# ---------------- STEP 1: Unique Table For (user_id, period) ----------------
df_user_period_labels = (
    df_score_with_targets_temp.sort_values(["user_id", "period"])
    .groupby(["user_id", "period"], as_index=False)
    .first()[["user_id", "period", "clusters_labelFa"]]
)
#%%

#%%

#%%

#%%

#%%

#%%

#%%

#%%
