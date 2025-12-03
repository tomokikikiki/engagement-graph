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
    file_path = "data/engagement.csv"
    if not os.path.exists(file_path):
        return pd.DataFrame()
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        st.error(f"Error loading CSV: {e}")
        return pd.DataFrame()

df_raw = load_data_from_csv()

# -------------------------------------------
# 3. サイドバー設定
# -------------------------------------------
st.sidebar.header("⚙️ 設定")

if df_raw.empty:
    st.warning("データファイル (data/engagement.csv) が見つかりません。")
    st.stop()

# 更新日時
try:
    file_stat = os.stat("data/engagement.csv")
    last_updated = pd.to_datetime(file_stat.st_mtime, unit='s') + pd.Timedelta(hours=9)
    st.sidebar.caption(f"最終更新: {last_updated.strftime('%Y-%m-%d %H:%M')}")
except:
    pass

st.sidebar.subheader("⚖️ スコアの重み付け")
w_slack = st.sidebar.slider("Slack (1投稿あたり)", 0.0, 0.5, 0.1, 0.01)
w_linear = st.sidebar.slider("Linear (1完了あたり)", 0.5, 5.0, 1.0, 0.1)

# -------------------------------------------
# 4. スコア計算
# -------------------------------------------
df_calc = df_raw.copy()

# NaN埋め（エラー防止）
df_calc["Slack Count"] = df_calc["Slack Count"].fillna(0)
df_calc["Linear Count"] = df_calc["Linear Count"].fillna(0)

df_calc["Slack Score"] = df_calc["Slack Count"] * w_slack
df_calc["Linear Score"] = df_calc["Linear Count"] * w_linear
df_calc["Total Score"] = df_calc["Slack Score"] + df_calc["Linear Score"]
df_calc["Productivity"] = df_calc["Total Score"] / df_calc["Working Hours"].replace(0, 1)

# ランキング順にソート (スコア0の人も含む)
df_ranked = df_calc.sort_values("Total Score", ascending=False).reset_index(drop=True)
df_ranked.index += 1

# -------------------------------------------
# 5. 可視化 (Dashboard)
# -------------------------------------------
st.title("📊 Team Engagement Graph")

# ★追加: 集計ステータスの表示
total_members = len(df_ranked)
active_members = len(df_ranked[df_ranked["Total Score"] > 0])
st.markdown(f"**集計対象: {total_members} 名** (うちスコア発生: {active_members} 名)")

col1, col2 = st.columns([1, 1])

with col1:
    st.subheader("📈 Engagement 内訳")
    
    # グラフ用にデータを整形
    df_chart = df_ranked[["User", "Slack Score", "Linear Score"]].melt(
        id_vars="User", var_name="Type", value_name="Score"
    )
    
    # 棒グラフ (全員を表示するために高さ制限を外す等の工夫は難しいが、データは渡す)
    # ※Streamlitの仕様上、0点のデータは棒が表示されませんが、スペースは確保されます
    st.bar_chart(
        df_chart,
        x="User",
        y="Score",
        color="Type",
        stack=True
    )
    
    st.info("※ 棒グラフはスコアが 0 のメンバーは表示されません。")

with col2:
    st.subheader("🏆 ランキング表")
    
    potential_cols = ["User", "Role", "Total Score", "Slack Count", "Linear Count"]
    display_cols = [c for c in potential_cols if c in df_ranked.columns]
    
    # ★変更点: height=800 を指定して、縦に長く表示する (スクロール減らす)
    st.dataframe(
        df_ranked[display_cols],
        use_container_width=True,
        height=800,  # 800pxの高さ確保
        column_config={
            "User": st.column_config.TextColumn("Name", width="medium"),
            "Total Score": st.column_config.ProgressColumn(
                "Score",
                format="%.1f",
                min_value=0,
                max_value=float(df_ranked["Total Score"].max()) * 1.1,
            ),
        }
    )

# デバッグ用
with st.expander("📝 全データのリストを確認"):
    st.dataframe(df_raw)