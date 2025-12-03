import streamlit as st
import pandas as pd
import os

# -------------------------------------------
# 1. ページ設定
# -------------------------------------------
st.set_page_config(page_title="Engagement Graph", layout="wide")

# パスワード認証
def check_password():
    if "app_password" not in st.secrets: return True
    pwd = st.text_input("🔑 Password", type="password")
    if pwd == st.secrets["app_password"]: return True
    if pwd: st.warning("Incorrect password")
    return False

if not check_password(): st.stop()

# -------------------------------------------
# 2. データ読み込み (CSVから)
# -------------------------------------------
@st.cache_data
def load_data_from_csv():
    """
    GitHub Actions等で生成されたCSVデータを読み込む
    """
    file_path = "data/engagement.csv"
    
    # ファイルがない場合のハンドリング
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
# 3. サイドバー設定
# -------------------------------------------
st.sidebar.header("⚙️ 設定")

# データがない場合の表示
if df_raw.empty:
    st.warning("データファイル (data/engagement.csv) が見つかりません。")
    st.info("💡 ヒント: 初回のデータ取得スクリプトが実行されるのを待つか、手動で `python scripts/update_data.py` を実行してください。")
    st.stop()

# 最終更新日時の表示
try:
    file_stat = os.stat("data/engagement.csv")
    last_updated = pd.to_datetime(file_stat.st_mtime, unit='s')
    last_updated_jst = last_updated + pd.Timedelta(hours=9)
    st.sidebar.caption(f"最終更新: {last_updated_jst.strftime('%Y-%m-%d %H:%M')}")
except:
    pass

st.sidebar.subheader("⚖️ スコアの重み付け")
w_slack = st.sidebar.slider("Slack (1投稿あたり)", 0.0, 0.5, 0.1, 0.01)
w_linear = st.sidebar.slider("Linear (1完了あたり)", 0.5, 5.0, 1.0, 0.1)

# -------------------------------------------
# 4. スコア計算
# -------------------------------------------
# ★ここが重要: 期間フィルタリング(Date)を行わず、CSVの値をそのまま使う

df_calc = df_raw.copy()

# スコア計算
df_calc["Slack Score"] = df_calc["Slack Count"] * w_slack
df_calc["Linear Score"] = df_calc["Linear Count"] * w_linear
df_calc["Total Score"] = df_calc["Slack Score"] + df_calc["Linear Score"]

# 生産性 (Score / Hour) ※0割り防止
df_calc["Productivity"] = df_calc["Total Score"] / df_calc["Working Hours"].replace(0, 1)

# ランキング順にソート
df_ranked = df_calc.sort_values("Total Score", ascending=False).reset_index(drop=True)
df_ranked.index += 1

# -------------------------------------------
# 5. 可視化 (Dashboard)
# -------------------------------------------
st.title("📊 Team Engagement Graph")
st.markdown("直近30日間のアクティビティ集計")

col1, col2 = st.columns([1, 1])

with col1:
    st.subheader("📈 Engagement 内訳")
    
    # グラフ用にデータを整形
    df_chart = df_ranked[["User", "Slack Score", "Linear Score"]].melt(
        id_vars="User", 
        var_name="Type", 
        value_name="Score"
    )
    
    # 積み上げ棒グラフ
    st.bar_chart(
        df_chart,
        x="User",
        y="Score",
        color="Type",
        stack=True
    )
    
    st.subheader("⏱ 稼働時間 vs 成果")
    # 散布図 (Role列がある場合のみ色分け)
    color_col = "Role" if "Role" in df_ranked.columns else None
    st.scatter_chart(
        df_ranked,
        x="Working Hours",
        y="Total Score",
        color=color_col,
        size="Productivity"
    )

with col2:
    st.subheader("🏆 ランキング表")
    
    # 表示用カラムの選定 (存在しないカラムは除外)
    cols = ["User", "Role", "Total Score", "Slack Count", "Linear Count", "Working Hours"]
    display_cols = [c for c in cols if c in df_ranked.columns]
    display_df = df_ranked[display_cols]
    
    # リッチなテーブル表示
    st.dataframe(
        display_df,
        use_container_width=True,
        column_config={
            "Total Score": st.column_config.ProgressColumn(
                "Score",
                format="%.1f",
                min_value=0,
                max_value=float(df_ranked["Total Score"].max()) * 1.1,
            ),
            "Slack Count": st.column_config.NumberColumn("Slack投稿"),
            "Linear Count": st.column_config.NumberColumn("Linear完了"),
        }
    )

# デバッグ用
with st.expander("📝 ソースデータ (CSV) を見る"):
    st.dataframe(df_raw)