# Occto pipeline ingestion

1. /disclaimer-agree → POST agreed=0で免責同意を通過する
2. そこで得たCookie(セッション)を以降のリクエストで保持し続ける
3. その状態で日付を指定してCSVのURLをたたく
4. 日付は1日単位

補足: Cookie保持だけでは足りず検索を挟む必要がある

Cookieを持ったまま/downloadCSVを直接たたくのではなく、その直前にPOST /info/hks/searchを実行する必要がある。

免責同意→/info/home→/info/hks/→POST /search(←ここ)→GET /downloadCsv
