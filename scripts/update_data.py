import os
import time
import pandas as pd
import requests
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from datetime import datetime, timedelta

# ------------------------------------------------------------------
# 1. Slackから「名簿」を作る関数
# ------------------------------------------------------------------
def fetch_slack_user_directory():
    token = os.environ.get("SLACK_TOKEN")
    if not token:
        print("Skipping Slack directory: Token missing.")
        return {}

    client = WebClient(token=token)
    try:
        users_resp = client.users_list()
    except SlackApiError as e:
        print(f"Error fetching users: {e}")
        return {}

    directory = {}
    for u in users_resp["members"]:
        if u["is_bot"] or u["deleted"] or "profile" not in u:
            continue
        email = u["profile"].get("email")
        if not email:
            continue
            
        is_guest = u.get("is_restricted", False) or u.get("is_ultra_restricted", False)
        directory[email] = {
            "User Name": u.get("real_name") or u["name"],
            "Role": "Contractor" if is_guest else "Employee",
            "Avatar": u["profile"].get("image_48", "")
        }
    return directory

# ------------------------------------------------------------------
# 2. Slackのメッセージ数を集計する関数 (スレッド対応版)
# ------------------------------------------------------------------
def fetch_slack_data(start_date, end_date):
    print("--- Fetching Slack Data (Including Threads) ---")
    token = os.environ.get("SLACK_TOKEN")
    channel_id = os.environ.get("SLACK_CHANNEL_ID")
    
    if not token or not channel_id:
        print("Token or Channel ID missing.")
        return pd.DataFrame(columns=["Email", "Slack Count"])

    client = WebClient(token=token)
    oldest = start_date.timestamp()
    latest = end_date.timestamp()
    
    try:
        # A. ユーザーID対応表
        users_resp = client.users_list()
        uid_to_email = {}
        for u in users_resp["members"]:
            if "profile" in u and "email" in u["profile"]:
                uid_to_email[u["id"]] = u["profile"]["email"]

        # B. 親メッセージ履歴取得
        # ※直近30日間の親メッセージを取得
        history = client.conversations_history(
            channel=channel_id, 
            oldest=oldest, 
            latest=latest,
            limit=1000
        )
        
        messages = history["messages"]
        print(f"Found {len(messages)} parent messages. Analyzing threads...")
        
        counts = {} 
        
        # C. メッセージを走査
        for i, msg in enumerate(messages):
            # システムメッセージやBot除外
            if "subtype" in msg or "bot_id" in msg:
                continue
            
            # --- 1. 親メッセージのカウント ---
            uid = msg.get("user")
            if uid in uid_to_email:
                email = uid_to_email[uid]
                counts[email] = counts.get(email, 0) + 1

            # --- 2. スレッド（返信）のカウント ---
            # thread_ts があり、かつ返信数が1以上の場合
            if "thread_ts" in msg and msg.get("reply_count", 0) > 0:
                try:
                    replies_resp = client.conversations_replies(
                        channel=channel_id,
                        ts=msg["thread_ts"],
                        limit=1000,
                        oldest=oldest, # 期間内の返信のみ対象にする
                        latest=latest
                    )
                    
                    for reply in replies_resp["messages"]:
                        # 親メッセージ自体の重複カウントを防ぐ
                        if reply["ts"] == msg["ts"]:
                            continue
                        
                        # Bot除外
                        if "bot_id" in reply:
                            continue
                            
                        r_uid = reply.get("user")
                        if r_uid in uid_to_email:
                            r_email = uid_to_email[r_uid]
                            counts[r_email] = counts.get(r_email, 0) + 1
                    
                    # APIレート制限対策 (重要)
                    time.sleep(0.1) 
                    
                except SlackApiError as e:
                    print(f"Thread fetch warning: {e}")
                    time.sleep(1) # エラー時は少し長く待つ
                    continue

            # 進捗ログ (50件ごと)
            if (i + 1) % 50 == 0:
                print(f"Processed {i + 1}/{len(messages)} threads...")

        return pd.DataFrame(list(counts.items()), columns=["Email", "Slack Count"])

    except SlackApiError as e:
        print(f"Slack API Error: {e.response['error']}")
        return pd.DataFrame(columns=["Email", "Slack Count"])

# ------------------------------------------------------------------
# 3. Linearの集計関数
# ------------------------------------------------------------------
def fetch_linear_data(start_date):
    api_key = os.environ.get("LINEAR_KEY")
    if not api_key:
        print("Skipping Linear data fetch: API Key missing.")
        return pd.DataFrame(columns=["Email", "Linear Count"])

    url = "https://api.linear.app/graphql"
    date_str = start_date.strftime("%Y-%m-%d")
    
    # 完了かつキャンセルされていないIssueを最大100件取得
    query = f"""
    query {{
      issues(
        first: 100
        filter: {{ 
          completedAt: {{ gte: "{date_str}" }}
          state: {{ type: {{ eq: "completed" }} }}
        }}
      ) {{
        nodes {{
          title
          assignee {{
            email
          }}
          completedAt
        }}
      }}
    }}
    """
    
    headers = {"Authorization": api_key, "Content-Type": "application/json"}
    
    try:
        response = requests.post(url, json={"query": query}, headers=headers)
        if response.status_code != 200:
            print(f"Linear API Error: {response.text}")
            return pd.DataFrame(columns=["Email", "Linear Count"])
            
        data = response.json()
        issues = data.get("data", {}).get("issues", {}).get("nodes", [])
        
        counts = {}
        for issue in issues:
            assignee = issue.get("assignee")
            if assignee and assignee.get("email"):
                email = assignee["email"]
                counts[email] = counts.get(email, 0) + 1
                
        return pd.DataFrame(list(counts.items()), columns=["Email", "Linear Count"])

    except Exception as e:
        print(f"Linear Connection Error: {e}")
        return pd.DataFrame(columns=["Email", "Linear Count"])

# ------------------------------------------------------------------
# 4. メイン実行処理
# ------------------------------------------------------------------
def main():
    print("🚀 Starting data update...")
    
    # 直近30日間を集計
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    print(f"📅 Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    # 1. データ取得
    user_directory = fetch_slack_user_directory()
    df_slack = fetch_slack_data(start_date, end_date)
    df_linear = fetch_linear_data(start_date)
    
    # 2. 名寄せ
    emails_slack = set(df_slack["Email"]) if not df_slack.empty else set()
    emails_linear = set(df_linear["Email"]) if not df_linear.empty else set()
    all_emails = set(user_directory.keys()) | emails_slack | emails_linear
    
    rows = []
    for email in all_emails:
        profile = user_directory.get(email, {
            "User Name": email, 
            "Role": "Unknown", 
            "Avatar": ""
        })
        
        # Slack Count
        slack_count = 0
        if not df_slack.empty:
            s_row = df_slack[df_slack["Email"] == email]
            if not s_row.empty:
                slack_count = s_row["Slack Count"].sum()
        
        # Linear Count
        linear_count = 0
        if not df_linear.empty:
            l_row = df_linear[df_linear["Email"] == email]
            if not l_row.empty:
                linear_count = l_row["Linear Count"].sum()
        
        rows.append({
            "Email": email,
            "User": profile["User Name"],
            "Role": profile["Role"],
            "Avatar": profile["Avatar"],
            "Slack Count": int(slack_count),
            "Linear Count": int(linear_count),
            "Working Hours": 40 if profile["Role"] == "Employee" else 20
        })
    
    if not rows:
        print("⚠️ No data found.")
        return

    df_merged = pd.DataFrame(rows)
    
    os.makedirs("data", exist_ok=True)
    output_path = "data/engagement.csv"
    df_merged.to_csv(output_path, index=False)
    print(f"✅ Saved to {output_path}")
    print(df_merged.head())

if __name__ == "__main__":
    main()