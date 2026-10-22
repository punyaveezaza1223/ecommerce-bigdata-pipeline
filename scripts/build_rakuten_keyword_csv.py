"""Build the offline Rakuten keyword plan without making network requests."""
import csv
import os
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
USER = ROOT / "config" / "rakuten_user_keywords.txt"
OUT = ROOT / "config" / "rakuten_japanese_keywords.csv"
TARGET = 3000

def clean(value: str) -> str:
    return unicodedata.normalize("NFKC", value).strip()

def classify(word: str) -> tuple[str, str]:
    groups = {
        "家電": ("パソコン", "スマホ", "カメラ", "テレビ", "冷蔵庫", "洗濯", "掃除", "エアコン", "イヤホン"),
        "家具": ("ソファ", "ベッド", "テーブル", "収納", "カーテン", "照明", "ラック"),
        "ファッション": ("シャツ", "パンツ", "バッグ", "靴", "スニーカー", "コート", "財布"),
        "美容": ("化粧", "美容", "シャンプー", "香水", "ネイル", "歯"),
        "スポーツ": ("ゴルフ", "釣り", "自転車", "キャンプ", "テント", "ヨガ", "ランニング"),
        "工具": ("工具", "ドリル", "レンチ", "のこ", "DIY", "塗料"),
        "食品": ("米", "コーヒー", "茶", "麺", "菓子", "調味"),
        "ペット": ("犬", "猫", "ペット", "水槽"),
        "本・趣味": ("本", "漫画", "模型", "楽器", "ギター", "フィギュア"),
    }
    for category, markers in groups.items():
        if any(marker in word for marker in markers):
            return category, "商品検索"
    return "生活用品", "商品検索"

def read_words(text: str) -> list[str]:
    return [clean(word) for word in text.replace("\n", ",").split(",") if clean(word)]

user_words = read_words(USER.read_text(encoding="utf-8"))
env_words = read_words(os.getenv("RAKUTEN_SEARCH_QUERIES", ""))
words: list[str] = []
seen: set[str] = set()
for word in user_words + env_words:
    if word not in seen:
        seen.add(word); words.append(word)

# Japanese shopping refinements: concrete specification searches, not synthetic IDs.
qualifiers = ["人気", "ランキング", "送料無料", "新品", "日本製", "大容量", "コンパクト", "業務用", "セット", "限定"]
for qualifier in qualifiers:
    for base in list(words):
        candidate = f"{base} {qualifier}"
        if candidate not in seen:
            seen.add(candidate); words.append(candidate)
        if len(words) >= TARGET:
            break
    if len(words) >= TARGET:
        break

if len(words) < TARGET:
    raise RuntimeError(f"Only {len(words)} distinct keywords available")

OUT.parent.mkdir(exist_ok=True)
with OUT.open("w", encoding="utf-8", newline="") as handle:
    writer = csv.DictWriter(handle, fieldnames=["category", "subcategory", "keyword_ja", "priority", "status", "last_page", "last_run_at"])
    writer.writeheader()
    for index, word in enumerate(words):
        category, subcategory = classify(word)
        writer.writerow({"category": category, "subcategory": subcategory, "keyword_ja": word, "priority": "high" if index < 300 else "normal", "status": "pending", "last_page": 0, "last_run_at": ""})
print(f"user={len(user_words)} env={len(env_words)} final={len(words)} output={OUT}")
