import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

# --- セキュリティ設定: パスワード認証 ---
def check_password():
    """パスワードが合致した場合のみTrueを返す"""
    # secretsにパスワードが設定されているか確認
    if "app_password" not in st.secrets:
        st.error("管理画面でパスワードが設定されていません。")
        return False
    
    # ユーザーに入力を求める
    password = st.text_input("🔑 アクセスパスワードを入力してください", type="password")
    
    if password == st.secrets["app_password"]:
        return True
    elif password:
        st.warning("パスワードが間違っています。")
    return False

# 認証チェック
if not check_password():
    st.stop()  # パスワードが違う場合はここで処理を停止（中身を見せない）

# -------------------------------------------
# 1. ページ設定とデータ生成 (実運用ではDBから取得)
# -------------------------------------------
st.set_page_config(page_title="Engagement Graph", layout="wide")

@st.cache_data
def load_data_from_csv():
    """
    GitHub Actions等で生成されたCSVデータを読み込む
    """
    file_path = "data/engagement.csv"
    
    if not os.path.exists(file_path):
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        st.error(f"Error loading CSV: {e}")
        return pd.DataFrame()

# データを読み込み
df_raw = load_data_from_csv()

# -------------------------------------------
# 2. サイドバー (設定・フィルタ)
# -------------------------------------------
st.sidebar.header("⚙️ 設定 & フィルタ")

# A. 期間選択 (デフォルトは直近1週間)
st.sidebar.subheader("📅 集計期間")
today = datetime.today()
last_week = today - timedelta(days=7)

date_range = st.sidebar.date_input(
    "期間を選んでください",
    value=(last_week, today), # デフォルト値
    max_value=today
)

# 期間フィルタリング処理
if len(date_range) == 2:
    start_date, end_date = date_range
    # DataFrameを期間で絞り込み
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    df_filtered = df_raw[(df_raw["Date"] >= start_date) & (df_raw["Date"] <= end_date)]
else:
    st.error("開始日と終了日を選択してください")
    st.stop()

# B. ウェイト調整 (シミュレーション用)
st.sidebar.subheader("⚖️ スコアの重み付け")
w_slack = st.sidebar.slider("Slack (1投稿あたり)", 0.0, 0.5, 0.1, 0.01)
w_linear = st.sidebar.slider("Linear (1完了あたり)", 0.5, 5.0, 1.0, 0.1)

# -------------------------------------------
# 3. データ集計ロジック
# -------------------------------------------
# ユーザーごとに合計を算出
df_grouped = df_filtered.groupby("User")[["Slack Count", "Linear Count", "Working Hours"]].sum().reset_index()

# スコア計算
df_grouped["Slack Score"] = df_grouped["Slack Count"] * w_slack
df_grouped["Linear Score"] = df_grouped["Linear Count"] * w_linear
df_grouped["Total Score"] = df_grouped["Slack Score"] + df_grouped["Linear Score"]

# 生産性 (Score / Hour) ※0割り防止
df_grouped["Productivity"] = df_grouped["Total Score"] / df_grouped["Working Hours"].replace(0, 1)

# ランキング順にソート
df_ranked = df_grouped.sort_values("Total Score", ascending=False).reset_index(drop=True)
df_ranked.index += 1 # 1位から始める

# -------------------------------------------
# 4. メインコンテンツ表示
# -------------------------------------------
st.title("📊 Team Engagement Graph")
st.markdown(f"集計期間: **{start_date.strftime('%Y-%m-%d')}** 〜 **{end_date.strftime('%Y-%m-%d')}**")

# カラム分け (左: グラフ, 右: ランキング)
col1, col2 = st.columns([1, 1])

with col1:
    st.subheader("📈 Engagement 内訳 (積上げ)")
    # グラフ用にデータを整形 (Melt)
    df_chart = df_ranked[["User", "Slack Score", "Linear Score"]].melt(
        id_vars="User", 
        var_name="Type", 
        value_name="Score"
    )
    
    # 棒グラフ表示 (SlackとLinearの色分け)
    st.bar_chart(
        df_chart,
        x="User",
        y="Score",
        color="Type",
        stack=True  # 積み上げグラフにする
    )

    st.subheader("⏱ 稼働時間 vs 成果 (散布図)")
    st.scatter_chart(
        df_ranked,
        x="Working Hours",
        y="Total Score",
        color="User",
        size="Productivity" # 円の大きさで生産性を表現
    )

with col2:
    st.subheader("🏆 ランキング表")
    
    # 表示するカラムを整理
    display_df = df_ranked[[
        "User", "Total Score", "Slack Count", "Linear Count", "Working Hours"
    ]]
    
    # リッチなテーブル表示 (進捗バーなどを付与)
    st.dataframe(
        display_df,
        use_container_width=True,
        column_config={
            "Total Score": st.column_config.ProgressColumn(
                "Engagement Score",
                help="SlackとLinearの加重平均スコア",
                format="%.1f",
                min_value=0,
                max_value=float(df_ranked["Total Score"].max()) * 1.1, # 最大値を少し余裕持たせる
            ),
            "Slack Count": st.column_config.NumberColumn("Slack投稿数"),
            "Linear Count": st.column_config.NumberColumn("Issue完了数"),
        }
    )

# -------------------------------------------
# 5. 生データ確認用 (アコーディオン)
# -------------------------------------------
with st.expander("📝 集計前の生データを見る"):
    st.dataframe(df_filtered)