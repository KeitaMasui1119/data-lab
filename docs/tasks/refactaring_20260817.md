# `src/main.py` リファクタリング提案（2026-08-17）

`src/` 直下にある唯一のPythonコードである `src/main.py` を対象に、コード改善案をまとめる。

CLAUDE.md は `src/main.py` を「sole CLI entry point ... Keep this as thin orchestration only」
と定義し、README.md 11行目も「orchestrated execution from a **thin** `src/main.py`」と説明して
いるが、実装は 1,641行・単一関数 1,510行に膨らんでおり、この設計意図から大きく乖離している。

---

## 0. 結論サマリ

| 優先度 | 件数 | 概要 |
|---|---|---|
| P1（構造/挙動） | 3件 | 単一1,510行関数、引数なし実行の副作用、ディスパッチ漏れの黙殺 |
| P2（重複/保守性） | 5件 | 日付パース21重複、3社×3レイヤーのコピペ、silver運用3コマンドの重複、マジックリテラル、テスト皆無 |
| P3（一貫性/コスト） | 3件 | import 105行と起動コスト、CLI入口の二重化、`resolve_default_target_date` の5重複 |

段階的に4フェーズへ分割して進める案を「3. リファクタリング案」に示す。
Phase 1〜3 は**振る舞い不変**の純粋な抽出、Phase 4 のみ挙動変更を含むため合意が必要。

---

## 1. 現状の計測値

すべて 2026-08-17 時点の `main`（`4ee7b3a`）で実測。

| 指標 | 値 | 備考 |
|---|---|---|
| ファイル行数 | 1,641行 | CLAUDE.md/coding-style の上限800行の**2倍超** |
| `main()` 関数の行数 | 1,510行（128〜1637行） | 同上限50行の**30倍** |
| import ブロック | 105行（1〜105行） | ファイル全体の6.4% |
| サブコマンド数 | 25個 | `subparsers.add_parser` |
| `add_argument` 呼び出し | 111回 | |
| `if args.command == ...` 分岐 | 24個（+ 1個は `in {None, ...}`） | `elif` ではなく独立した `if` |
| `date.fromisoformat` | 25回 | |
| `parser.error(f"Invalid ...")` | 21回 | ほぼ同一文言 |
| `RustFSClient()` | 15回 | |
| `"jp-power-grid-dev"` リテラル | 15回 | |
| `"dlh_dev"` リテラル | 14回 | |
| `scraper.close()` | 7回 | `try/finally` ごと重複 |
| `main.py` を参照するテスト | **0件** | `grep -rl "import main\|from main" tests/` が空 |

lint / 型チェックは現状すべてパスしている（`ruff check`、`ruff format --check`、`pyright` とも
エラー0）。つまり本提案は「壊れているものを直す」話ではなく、**規約上の上限を大幅に超えた構造を
規約内に戻し、今後コマンドを追加し続けられる形にする**話である。

---

## 2. 検出した問題

### 2.1 [P1] `main()` が単一の1,510行関数

`main()`（128〜1637行）が、25個すべてのサブパーサ定義（132〜849行）と、25個すべての
ディスパッチ本体（853〜1637行）を1つの関数スコープに抱えている。

具体的な弊害：

**(a) 変数がコマンド間でスコープを共有している。**
`result` が9回、異なる型に再束縛される（891, 1101, 1149, 1186, 1222, 1258, 1432, 1472行）。
後から追加された4コマンドは衝突を避けるため、
`power_usage_hokuriku_silver_result`（1514行）、`tohoku_silver_result`（1558行）、
`chugoku_silver_result`（1573行）、`shikoku_silver_result`（1588行）という冗長な名前を使っている。
`target_date` / `from_date` / `to_date` / `rustfs` / `scraper` も同様に全コマンドで共有される。

**(b) ヘルパー関数の定義位置が実行順序に縛られている。**
`_parse_sda_silver_date_range()` は 1531行で `def` されるが、これは `main()` 本体の逐次実行の
一部であり、**1531行より前のコードからは呼べない**。その結果、同一処理を必要とする
`ingest-occto-bronze-to-silver`（1450〜1470行）と
`ingest-power-usage-hokuriku-bronze-to-silver`（1492〜1512行）は、ヘルパーが存在するにも
かかわらずインラインでコピーを持っている。関数化しても再利用できない構造になっている。

**(c) 分岐が `elif` ではなく独立した `if` の連鎖。**
コマンド名がユニークなので現状は正しく動くが、25個の条件が毎回すべて評価される。
また `bootstrap-storage` だけが `return`（880行）で早期脱出し、他は素通りするという
非対称な作りになっている。

### 2.2 [P1] 引数なし実行が `bootstrap-storage` を実行してしまう

```python
# src/main.py:853-858
if args.command in {None, "bootstrap-storage"}:
    rustfs = RustFSClient()
    if args.command is None:
        logger.info(
            "No command was provided. Running bootstrap-storage for compatibility."
        )
```

`uv run python src/main.py` を引数なしで叩くと、ヘルプではなく**バケット作成（Object Lock 設定を
含むインフラ provisioning）が実行される**。タイプミスやシェル履歴の誤爆で副作用のある処理が
走る設計であり、CLI の慣例（引数なし → usage 表示）にも反する。

860行の `getattr(args, "buckets", None)` という防御的な `getattr` も、この fallback のために
必要になっているもの（`args.command is None` のときはサブパーサが走らず `buckets` 属性が
存在しないため）。

なお、README.md・CI設定・compose のいずれもこの引数なし形式を使っておらず（すべて明示的に
サブコマンドを渡している）、**この互換性 fallback に依存している呼び出し元は現時点で存在しない**
ことを確認済み。

### 2.3 [P1] ディスパッチ漏れが exit 0 で素通りする

25個の `if` 連鎖の末尾に `else` に相当する分岐がなく（最後は1601行の
`run-occto-orchestrator`、次は1640行の `if __name__`）、`main()` は常に `None` を返す。

このため、**サブパーサだけ追加してディスパッチ本体を書き忘れると、何もせず正常終了（exit 0）する**。
コマンドが25個に達し今後も増える構造で、この検出漏れは実害になりやすい。

関連して、オーケストレーターの実行結果は `logger.info` に流されるだけで終了コードに反映されない
（1016〜1022行、1631〜1637行）。現状 `PipelineStepResult.status` は `"success"` と `"skipped"` しか
生成されず `"failed"` は未実装のため実害は出ていないが、`tasks.md` 8.4「実行結果判定の厳格化」で
`status="failed"` を導入する際は、**終了コードへ反映する受け皿が `main.py` 側に必要**になる。

### 2.4 [P2] 日付パースの重複（21箇所）

以下のブロックが、変数名と引数名だけを変えて21回繰り返されている。

```python
try:
    target_date = date.fromisoformat(args.target_date)
except ValueError as exc:
    parser.error(f"Invalid --target-date value: {args.target_date} ({exc})")
```

さらに `--from-date` / `--to-date` / `--target-date` の3択を解決する
「範囲パース」ブロック（下記）は、完全に同一の形で3回出現する
（1450〜1470行、1492〜1512行、1531〜1554行 = `_parse_sda_silver_date_range`）。

```python
from_date = None
to_date = None
if args.from_date:
    ... from_date をパース ...
    if args.to_date: ... to_date をパース ...
    else: to_date = from_date
elif args.target_date:
    ... from_date をパース ...
    to_date = from_date
```

### 2.5 [P2] 3社 × 3レイヤー = 9ブロックがほぼ同一

東北・中国・四国の `supply_demand_actuals` は、Raw/Bronze/Silver の各レイヤーで
**会社名・スクレイパークラス・呼び出す関数・ログ文言だけが違う**ブロックが3つずつ並んでいる。

| レイヤー | 該当行 | 1ブロックの行数 | 合計 |
|---|---|---|---|
| scrape（Raw） | 1174〜1280 | 約35行 | 約107行 |
| raw-to-bronze | 1282〜1346 | 約21行 | 約65行 |
| bronze-to-silver | 1556〜1599 | 約14行 | 約44行 |

引数定義側はすでに `_add_sda_bronze_arguments()`（441行）と
`_add_sda_silver_arguments()`（492行）で共通化されているのに、**ディスパッチ側だけ共通化されて
いない**。会社を1社増やすたびに3ブロック・約70行のコピペが必要な状態で、
`tasks.md` 1節には関西・北海道・沖縄・中部・東京の追加が残っている。

同様に `scrape-occto`（1096〜1123行）と `scrape-power-usage-hokuriku`（1144〜1172行）も、
「日次ループ + `try/finally` で `scraper.close()` + skipped/saved のログ分岐」という
同一構造をそれぞれインラインで持っている。

### 2.6 [P2] silver 運用3コマンドの重複

`provision-silver-tables`（1348行）、`evolve-silver-partition-spec`（1366行）、
`expire-silver-snapshots`（1385行）は、いずれも冒頭で同じ4ステップを繰り返す。

```python
schema_dir = Path(args.schema_dir)
if not schema_dir.exists():
    parser.error(f"Schema directory does not exist: {schema_dir}")
schema_files = sorted(schema_dir.rglob("*.csv"))
if not schema_files:
    parser.error(f"No schema CSV files found in: {schema_dir}")
...
table_identifier = f"silver.{schema_file.stem}"
```

「スキーマCSVのファイル名 = テーブル名」という**暗黙の規約が3箇所に散っている**ため、
命名規則を変えるときの修正漏れが起きやすい。実際、直近の PR #82 でスキーマCSVを
データセット別サブフォルダへ移した際は、この3箇所すべてで `glob` → `rglob` の変更が必要だった。

### 2.7 [P2] マジックリテラルの散在

| リテラル | 出現回数 | 該当行の例 |
|---|---|---|
| `"jp-power-grid-dev"` | 15 | 各 `--bucket` の default |
| `"dlh_dev"` | 14 | 各 `--catalog` の default |
| `.../bronze/jepx_spot_price/jepx_spot_price.csv` | 3 | 198, 243, 278 |
| `.../bronze/occto_unit_generation_actuals/...csv` | 2 | 371-374, 626-629 |
| `"/workspace/configuration/iceberg/schema/silver"` | 3 | 764, 783, 802 |
| `"/workspace/src/dbt/jepx_power"` | 1 | 288 |

一方で silver 側のスキーマディレクトリは各パイプラインモジュールが公開する定数
（`DEFAULT_SILVER_SCHEMA_DIR`、`OCCTO_DEFAULT_SILVER_SCHEMA_DIR`、
`POWER_USAGE_HOKURIKU_DEFAULT_SILVER_SCHEMA_DIR`）を import して使っており、
**bronze 側だけリテラル直書きという非対称**になっている。CLAUDE.md が
`configuration/iceberg/schema/` を「Source of truth for all table schemas」と定めている以上、
パス解決も1箇所に集約すべき。

補足：198・243・278行はいずれも101文字で、`ruff.toml` の `line-length = 88` を超えている。
ruff の E501 が「空白を含まない1語からなる行」を除外する仕様のため lint はパスしているが、
規約上の上限は超えている。

### 2.8 [P2] `main.py` にテストが1件も存在しない

`grep -rl "import main\|from main" tests/` の結果は0件。リポジトリ唯一のCLI入口であり、
25コマンド・111引数の引数解決・バリデーション・デフォルト値決定ロジックを持つファイルが、
**完全にテストされていない**。共通ルールの「最低カバレッジ80%」に対しても大きな穴になっている。

`tasks.md` 8.2 が要求している
「`--silver-all-fiscal-years` と `--silver-fiscal-year` の同時指定が `parser.error` で弾かれること」
のテストも、この入口がテスト可能な形になっていないことが障害になっている
（現状 `main()` は `sys.argv` を直接読み、`parser` をローカル変数に閉じ込めているため、
テストから paser 単体を取り出せない）。

なお、このバリデーション自体も `src/main.py:983` と
`src/orchestration/pl_jepx_spot_price.py:339` の2箇所に重複している。

### 2.9 [P3] import ブロック105行と起動コスト

1〜105行の import により、`--help` を表示するだけでも polars / pyarrow / pyiceberg / duckdb /
boto3 / requests がすべてロードされる。25コマンドのうち実際に使うのは1つだけなので、
起動時間の大半が無駄になっている。

また `resolve_default_target_date` が5つのモジュールに同名で存在するため、
4回の as-import による別名付け（45, 52-54, 59-61, 66-68, 73-75行）が必要になっており、
import ブロックの読みにくさを増している。

### 2.10 [P3] CLI入口が二重化している（CLAUDE.md との乖離） — ✅ 解消済み

CLAUDE.md は `src/main.py` を「**sole** CLI entry point」と定めているが、当時は
以下7モジュールが独自の `argparse` + `if __name__ == "__main__"` を持ち、第2のCLI表面を
形成していた。

| モジュール | 対応 |
|---|---|
| `src/orchestration/pl_jepx_spot_price.py` | PR #105 で撤去 |
| `src/pipeline/raw/source_to_raw_jepx_spot_price.py` | PR #105 で撤去 |
| `src/pipeline/bronze/source_to_bronze_jepx_spot_price.py` | PR #105 で撤去 |
| `src/pipeline/silver/bronze_to_silver_jepx_spot_price.py` | PR #105 で撤去 |
| `src/orchestration/pl_occto_unit_generation_actuals.py` | PR #110 で撤去 |
| `src/pipeline/bronze/upload_raw.py` | PR #110 でモジュールごと削除 |
| `src/setup/manage_iceberg.py` | **意図的に残す**（下記） |

JEPX 期に作られたモジュールと OCCTO オーケストレーターに集中しており、後発の
`power_usage_hokuriku` / `supply_demand_actuals` 系は独自CLIを持っていなかった。
つまり**方針は既に「main.py に集約」へ移っており、旧世代のモジュールだけが取り残されていた**。
`--silver-all-fiscal-years` バリデーションの二重実装（2.8）はこの乖離の実害の一例。

例外として `src/setup/manage_iceberg.py` は、テーブル作成という運用系の別コマンドとして
`README.md:282` と `docs/architecture/data_model.md:159` が
`python -m setup.manage_iceberg table create ...` の直接実行を正規の手順として案内しており、
これは残す判断でよい（README.md:304 も「`manage_iceberg.py` admin CLI」と明記）。

`upload_raw.py` だけは撤去ではなく削除になった。`upload_raw_file()` はどこからも
import されておらず、それが呼ぶ `common/raw_object_io.upload_local_file()` も
この1箇所からしか使われていなかったため、`__main__` を外すとモジュール全体が
到達不能になる。手動アップロードは `mc` / `aws s3 cp` で足りるので、両方削除した
（`read_object_text()` は bronze 6モジュールが使うので `raw_object_io.py` 自体は残存）。

撤去で失われるカバレッジは移設した。フラグ排他バリデーションは
`tests/test_main_cli.py` が既にCLI経由で検証しており、モジュール `main()` の
「失敗ステップは非ゼロ終了する」テスト2件は `tests/cli/commands/test_occto.py` へ移した
（JEPX側の同名テストが `tests/cli/commands/test_jepx.py` に置かれているのと同じ形）。

### 2.11 [P3] `resolve_default_target_date` が5モジュールに重複 — ✅ 解消済み

当時、以下5モジュールが同一の本体（docstring 含めてバイト一致）を持っていた。

```
src/pipeline/raw/source_to_raw_occto_unit_generation_actuals.py:75
src/pipeline/raw/source_to_raw_power_usage_hokuriku.py:39
src/pipeline/raw/source_to_raw_supply_demand_actuals_tohoku.py:40
src/pipeline/raw/source_to_raw_supply_demand_actuals_chugoku.py:38
src/pipeline/raw/source_to_raw_supply_demand_actuals_shikoku.py:40
```

```python
jst_now = now.astimezone(ZoneInfo("Asia/Tokyo"))
return (jst_now - timedelta(days=1)).date()
```

「JSTの前日」というデータセット非依存の純粋なロジックであり、CLAUDE.md の
「`src/common/` — Dataset-agnostic shared primitives only」の定義に照らせば
`src/common/`（すでに `resolve_target_at` が同じ理由で集約されている）に
置くべきもの。

**関数本体は既に `src/common/utils.py`（旧 `utilities.py`）へ集約済み**で、5モジュールには
移動先を指す注記コメントだけが残っている。ただし**テスト側の重複は PR #111 まで残っていた** ——
`test_resolve_default_target_date_is_yesterday_in_jst` が5つのテストファイルに複製され、
うち3つはバイト一致だった。これを `tests/common/test_utils.py` に統合した際、
5つとも JST-aware な datetime しか渡しておらず、**実際の呼び出し経路である UTC 入力
（全呼び出し元が `datetime.now(UTC)` を渡す）が一度もテストされていなかった**ことが判明したため、
15:00 UTC 以降は既に JST の翌日である境界ケースを追加した。

---

## 3. リファクタリング案

### 目標構成

```
src/
  main.py                    # 約60行: レジストリからパーサを組み立てディスパッチするだけ
  cli/
    __init__.py
    registry.py              # CommandSpec と COMMANDS レジストリ
    defaults.py              # DEFAULT_BUCKET / DEFAULT_CATALOG / SCHEMA_ROOT など
    args.py                  # add_bucket_arg / add_catalog_arg / add_date_range_args
    dates.py                 # parse_iso_date / parse_date_range
    commands/
      storage.py             # bootstrap-storage
      jepx.py                # scrape-jepx, ingest-jepx-*, run-jepx-*
      occto.py               # scrape-occto, ingest-occto-*, run-occto-orchestrator
      power_usage.py         # scrape/ingest power-usage-hokuriku
      supply_demand.py       # 3社をテーブル駆動で生成
      silver_admin.py        # provision / evolve / expire
```

`CommandSpec` の形：

```python
@dataclass(frozen=True)
class CommandSpec:
    name: str
    help: str
    configure: Callable[[argparse.ArgumentParser], None]   # add_argument 群
    handler: Callable[[argparse.Namespace], int | None]    # 実処理、終了コードを返す
```

`main.py` はこれだけになる：

```python
def main() -> int:
    parser = argparse.ArgumentParser(description="Data platform orchestrator")
    subparsers = parser.add_subparsers(dest="command")
    for spec in COMMANDS:
        sub = subparsers.add_parser(spec.name, help=spec.help)
        spec.configure(sub)

    args = parser.parse_args()
    if args.command is None:
        parser.print_help()
        return 2

    handler = COMMAND_BY_NAME[args.command].handler   # KeyError = 登録漏れの即時検出
    return handler(args) or 0
```

### Phase 1 — 回帰ネットを張る（振る舞い不変・テストのみ）

**先にテストを書く。** 現状 `main.py` はテスト0件なので、抽出を始める前に振る舞いを固定する。

- `tests/test_main_cli.py` を新設し、以下を検証：
  - 25コマンドすべてについて `parser.parse_args([cmd, ...最小引数])` が成功する
  - 各コマンドの default 値（bucket / catalog / schema-path）が現状値と一致する
  - `--silver-all-fiscal-years` × `--silver-fiscal-year` の同時指定が `SystemExit` になる
    （`tasks.md` 8.2 の未消化タスクをここで回収できる）
  - `--target-date 2026-13-99` のような不正日付が `SystemExit` になる
- そのために `main()` からパーサ構築部を `build_parser() -> argparse.ArgumentParser` として
  切り出す（この時点ではまだ `main.py` 内で完結してよい）。

### Phase 2 — 純粋な抽出（振る舞い不変）

- `src/cli/defaults.py` に `DEFAULT_BUCKET` / `DEFAULT_CATALOG` / `SCHEMA_ROOT` を定義し、
  15+14+9箇所のリテラルを置換する。bronze スキーマパスも
  `SCHEMA_ROOT / "bronze" / <dataset> / <file>.csv` の形で解決する。
- `src/cli/dates.py` に `parse_iso_date(parser, value, flag)` と
  `parse_date_range(parser, args)` を実装し、21+3箇所の重複を置換する。
- `src/cli/args.py` に `add_bucket_arg` / `add_catalog_arg` / `add_date_range_args` を実装し、
  111回の `add_argument` のうち共通のものを集約する。
- `resolve_default_target_date` を `src/common/utils.py` へ集約し、5モジュールの重複を削除、
  main.py の4つの as-import も解消する（2.11）。

この段階で `main.py` は概算 1,641行 → 900行程度になる見込み。

### Phase 3 — コマンド単位のモジュール分割 + テーブル駆動化

- 上記「目標構成」に沿って `src/cli/commands/*.py` へ分割し、`registry.py` に集約する。
- `supply_demand.py` では3社を宣言テーブルに落とす：

  ```python
  @dataclass(frozen=True)
  class SdaCompany:
      name: str
      scraper_cls: type[BaseHttpScraper]
      scrape: Callable[..., object]
      ingest: Callable[..., int]
      to_silver: Callable[..., object]

  SDA_COMPANIES = (SdaCompany("tohoku", ...), SdaCompany("chugoku", ...), SdaCompany("shikoku", ...))
  ```

  9ブロック・約216行が、1つのパラメータ化されたハンドラ3種 + 3行のテーブルに縮む。
  会社追加時の変更は**テーブルに1行**だけになる。
- `scrape-occto` / `scrape-power-usage-hokuriku` の日次ループ + `try/finally` +
  skipped/saved ログを `run_daily_scrape_loop()` として共通化する。
- silver 運用3コマンドの前処理を `iter_silver_schema_files(parser, schema_dir)` に集約し、
  「CSVファイル名 = テーブル名」規約を1箇所に閉じ込める（2.6）。

この段階で `main.py` は約60行、各コマンドモジュールは50〜150行に収まり、
CLAUDE.md の「thin orchestration only」と coding-style の800行/50行制限を満たす。

### Phase 4 — 挙動変更（**要合意**）

以下は互換性を壊すため、実施可否を個別に判断する。

1. **引数なし実行を usage 表示 + exit 2 に変える**（2.2）。
   README・CI・compose のいずれもこの形式を使っていないことは確認済みだが、
   ユーザーのシェル履歴やローカルスクリプトに残っている可能性はある。
2. **未登録コマンドを即時エラーにする**（2.3）。レジストリ化（Phase 3）で自動的に達成される。
3. **ハンドラの戻り値を終了コードにする**（`sys.exit(main())`）。
   `tasks.md` 8.4 で `status="failed"` を導入する際の受け皿になる。
4. ~~**旧世代モジュールの独自 `__main__` を撤去する**（2.10）。~~ ✅ **完了**
   PR #105 が JEPX 系4モジュール、PR #110 が OCCTO オーケストレーターを撤去し、
   `upload_raw.py` は到達不能になるためモジュールごと削除した。
   `manage_iceberg.py` は運用コマンドとして意図的に直接実行されているため対象外のまま。
   これにより `src/main.py` が CLAUDE.md の言う唯一のCLI入口になった。

---

## 4. 検証手順

各フェーズ完了時に以下を実行する。

```bash
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
uv run pyright
uv run pytest tests/ -q
```

加えて、Phase 2・3 は振る舞い不変であることを次で担保する。

1. Phase 1 で追加した `tests/test_main_cli.py` が全フェーズを通じてグリーンであること。
2. リファクタ前後で全25コマンドのヘルプ出力が一致すること：

   ```bash
   for c in $(uv run python src/main.py --help | ...); do
       uv run python src/main.py "$c" --help
   done > /tmp/help_before.txt   # リファクタ後に diff で比較
   ```

3. 実データを伴う経路（`scrape-*` / `ingest-*`）は、RustFS 起動環境で
   `scrape-supply-demand-actuals-tohoku --target-date <既取得日>` を1本流し、
   sha256差分によるskipログが従来どおり出ることを確認する。

---

## 5. スコープ外

以下は本提案には含めない。

- `src/pipeline/` 配下・`src/common/` 配下のリファクタリング
  （今回の対象は「`src/` 直下のPythonコード」= `main.py` のため）。
  ただし 2.11 の `resolve_default_target_date` 集約だけは、main.py の import 重複を
  解消するために Phase 2 に含めている。
- `tasks.md` 8.4「実行結果判定の厳格化」の本体実装
  （`PipelineStepResult` に想定行数/実測行数を持たせる部分）。
  本提案は終了コードの受け皿を用意するところまで。
- `tasks.md` 8.5「バックフィル用CLIコマンド」の追加。
  ただし Phase 3 完了後は `src/cli/commands/jepx.py` に1ハンドラを足すだけになり、
  実装コストは大きく下がる。
