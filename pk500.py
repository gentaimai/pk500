import re
import time
import csv
from datetime import datetime, timezone
from dataclasses import dataclass
from urllib.parse import urljoin
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

import random
from typing import Optional

try:
    import cloudscraper  # type: ignore
except ImportError:
    cloudscraper = None

import unicodedata

# ---- 調整パラメータ ----
SLEEP_SEC = 2.0          # リクエスト間隔（負荷軽減）
MAX_SETS = None 
ONLY_POKEMON = True      # ポケモンに絞る（指数用途なら通常True）
DEBUG_LIMIT_CARDS = None # デバッグ用に読み込みカード枚数を制限するときに使う。例: 20とか 
# ------------------------

def current_run_times():
    """
    Returns aware datetime along with human-friendly and ISO8601 strings (with timezone).
    """
    run_dt = datetime.now(timezone.utc).astimezone()
    human = run_dt.strftime("%Y-%m-%d %H:%M:%S %Z%z")
    iso = run_dt.isoformat()
    return run_dt, human, iso

def is_pokemon_text(s: str) -> bool:
    """
    'Pokémon' のアクセント違い等を吸収しつつ判定する。
    - Pokémon / Pokemon の両方OK
    - 大文字小文字無視
    """
    if not s:
        return False
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))  # é 等の結合文字除去
    s = s.lower()
    return "pokemon" in s


BASE = "https://www.psacard.com"
HEADERS = {
    # Use a mainstream browser UA; prior custom UA could trigger 403 on psa site.
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.psacard.com/",
}


@dataclass
class CardValue:
    name: str
    url: str
    value_usd: float

session = requests.Session()
session.headers.update(HEADERS)

scraper = None
if cloudscraper is not None:
    # cloudscraper helps bypass basic Cloudflare/browser checks (common on PSA site)
    scraper = cloudscraper.create_scraper(
        browser={
            "browser": "chrome",
            "platform": "windows",
            "mobile": False,
        },
    )
    scraper.headers.update(HEADERS)


def fetch(
    url: str,
    retries: int = 7,
    timeout: tuple[float, float] = (10.0, 60.0),  # (connect, read)
    base_delay: float = 1.0,
    max_delay: float = 60.0,
) -> BeautifulSoup:
    """
    Fetch with retry + exponential backoff + jitter.
    - Retries on transient network errors (timeouts/connection), 403 (possible CF), 429, and 5xx.
    - Uses full jitter: sleep = uniform(0, cap) where cap grows exponentially.
    - Uses session first, then cloudscraper on 403 (and keeps using it if needed).
    """
    last_exc: Optional[BaseException] = None

    def _sleep_with_backoff(attempt: int, retry_after: Optional[str] = None) -> None:
        # Honor Retry-After when provided (seconds). If invalid, ignore.
        if retry_after:
            try:
                ra = float(retry_after)
                time.sleep(min(ra, max_delay))
                return
            except ValueError:
                pass

        cap = min(max_delay, base_delay * (2 ** (attempt - 1)))
        # full jitter: random between 0 and cap
        time.sleep(random.uniform(0, cap))

    for attempt in range(1, retries + 1):
        try:
            # 1) Try normal session first
            resp = session.get(url, timeout=timeout)

            # 2) On 403, try cloudscraper (often helps with CF/browser checks)
            if resp.status_code == 403 and scraper:
                resp = scraper.get(url, timeout=timeout)

            # Retryable HTTP statuses
            if resp.status_code in (403, 429, 500, 502, 503, 504):
                if attempt == retries:
                    resp.raise_for_status()

                retry_after = resp.headers.get("Retry-After")
                _sleep_with_backoff(attempt, retry_after=retry_after)
                continue

            # Other HTTP errors are non-retryable
            resp.raise_for_status()

            # Normal pacing to reduce load (in addition to backoff on failures)
            time.sleep(SLEEP_SEC)
            return BeautifulSoup(resp.text, "lxml")

        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout) as e:
            last_exc = e
        except requests.exceptions.ConnectionError as e:
            last_exc = e
        except requests.exceptions.HTTPError as e:
            # If it's a retryable code, retry; else raise immediately
            status = getattr(e.response, "status_code", None)
            if status in (403, 429, 500, 502, 503, 504):
                last_exc = e
            else:
                raise

        # If we got here, we are going to retry (unless last attempt)
        if attempt == retries:
            raise last_exc  # type: ignore[misc]

        _sleep_with_backoff(attempt)

    # Should not reach here
    raise last_exc  # type: ignore[misc]

def money_to_float(s: str) -> float:
    # "$5,777.50" -> 5777.50, "—" -> 0
    s = s.strip()
    if not s or s in {"-", "—", "N/A"}:
        return 0.0
    s = s.replace("$", "").replace(",", "")
    try:
        return float(s)
    except ValueError:
        return 0.0

def int_from(s: str) -> int:
    s = s.strip().replace(",", "")
    try:
        return int(s)
    except ValueError:
        return 0

def get_tcg_root_url() -> str:
    # AuctionPrices の “TCG一覧トップ” は 404 なので、
    # Population(POP) の TCGルートからセット一覧を辿る
    return urljoin(BASE, "/pop/tcg-cards/156940")


def iter_set_urls(pop_root: str):
    soup = fetch(pop_root)

    # 年ページ: /pop/tcg-cards/2025/290436 のような形式
    year_urls = []
    for a in soup.select('a[href^="/pop/tcg-cards/"]'):
        href = a.get("href", "")
        if re.fullmatch(r"/pop/tcg-cards/\d{4}/\d+", href):
            year_urls.append(urljoin(BASE, href))

    year_urls = sorted(set(year_urls))
    print(f"Year pages found: {len(year_urls)}")

    count_sets = 0
    for yurl in year_urls:
        ysoup = fetch(yurl)

        # セットURL: /pop/tcg-cards/{year}/{set-slug}/{set-id}
        for a in ysoup.select('a[href^="/pop/tcg-cards/"]'):
            href = a.get("href", "")
            m = re.fullmatch(r"/pop/tcg-cards/(\d{4})/([^/]+)/(\d+)", href)
            if not m:
                continue

            # ★ここが重要：リンクテキストで「Pokemon系」だけ残す
            if ONLY_POKEMON:
                link_text = (a.get_text() or "").strip()
                if not is_pokemon_text(link_text):
                    continue

            slug, set_id = m.group(2), m.group(3)
            apr_set_url = urljoin(BASE, f"/auctionprices/tcg-cards/{slug}/{set_id}")
            yield apr_set_url

            count_sets += 1
            if MAX_SETS is not None and count_sets >= MAX_SETS:
                return



def iter_card_urls_in_set(set_url: str):
    """
    セットページ内のカード一覧をページングしてカードURLを拾う。
    カードURL例:
      /auctionprices/tcg-cards/<set>/<subject>/<item-id>
    """
    next_url = set_url
    seen = set()

    while next_url:
        soup = fetch(next_url)

        # セット内のカード行リンク
        for a in soup.select('a[href^="/auctionprices/tcg-cards/"]'):
            href = a["href"]
            # カードページ: 末尾が数字IDのことが多い
            if re.search(r"/auctionprices/tcg-cards/.+/\d+$", href):
                card_url = urljoin(BASE, href)
                if card_url not in seen:
                    seen.add(card_url)
                    yield card_url

        # セット内ページング
        next_url = None
        # "Next" ボタン探し（セット例ページにある pagination を想定）
        for a in soup.select("a[href]"):
            if (a.get_text() or "").strip().lower() in {"next", "next >", ">", "→"}:
                href = a.get("href", "")
                if href and href.startswith("/auctionprices/tcg-cards"):
                    next_url = urljoin(BASE, href)
                    break

def parse_card_value(card_url: str) -> CardValue | None:
    soup = fetch(card_url)

    # タイトル（h1が取れないページがあるので <title> も使う）
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        name = h1.get_text(" ", strip=True)
    else:
        t = soup.find("title")
        name = t.get_text(" ", strip=True) if t else card_url
        # <title> には "Auction Prices Realized - ..." みたいな接頭辞が付くことがあるので軽く整形
        name = re.sub(r"\s*\|\s*PSA.*$", "", name).strip()


    # "Auction Prices By Grade" テーブルを探す
    # 実装方針: "Average Price" と "Population" を含むtableを特定
    tables = soup.find_all("table")
    target = None
    for t in tables:
        th_text = " ".join([th.get_text(" ", strip=True) for th in t.find_all(["th"])])
        if "Average Price" in th_text and "Population" in th_text:
            target = t
            break

    if target is None:
        # 価格データが無いカードもある（auction results 0件等）
        return None

    # 列インデックスを特定
    headers = [th.get_text(" ", strip=True) for th in target.find_all("th")]
    def col_idx(key: str) -> int:
        for i, h in enumerate(headers):
            if key.lower() in h.lower():
                return i
        return -1

    idx_avg = col_idx("Average Price")
    idx_pop = col_idx("Population")
    idx_grade = col_idx("Grade")
    if idx_avg < 0 or idx_pop < 0:
        return None

    def is_grade_10(text: str) -> bool:
        text = text.strip()
        if not text:
            return False
        # PSA表記ゆれ（"GEM-MT 10", "Gem Mint 10", "10" など）をまとめて拾う
        return bool(re.search(r"\b10\b", text))

    grade_col = idx_grade if idx_grade >= 0 else 0  # Grade列が取れない場合は先頭列をGrade扱い
    value = 0.0
    rows = target.find_all("tr")
    for r in rows[1:]:
        cols = [c.get_text(" ", strip=True) for c in r.find_all(["td", "th"])]
        if len(cols) <= max(idx_avg, idx_pop):
            continue
        grade_text = cols[grade_col] if len(cols) > grade_col else ""
        if not is_grade_10(grade_text):
            continue
        avg = money_to_float(cols[idx_avg])
        pop = int_from(cols[idx_pop])
        if avg > 0 and pop > 0:
            value += avg * pop

    if value <= 0:
        return None

    return CardValue(name=name, url=card_url, value_usd=value)


def compute_index(values_desc: list[CardValue]) -> dict:
    """
    values_desc: value_usd 降順にソート済みの CardValue リスト
    """
    n = len(values_desc)
    if n == 0:
        return {
            "n_total": 0,
            "k_used": 0,
            "sum_value": 0.0,
            "divisor_10000": None,
            "index_level": None,
        }

    if n < 500:
        k = max(1, n // 2)   # 上位半分（最低1）
    else:
        k = 500

    basket = values_desc[:k]
    s = sum(x.value_usd for x in basket)

    # 10,000基準に正規化する divisor（初回用）
    divisor = s / 10000.0 if s > 0 else None
    index_level = (s / divisor) if divisor else None  # = 10000

    return {
        "n_total": n,
        "k_used": k,
        "sum_value": s,
        "divisor_10000": divisor,
        "index_level": index_level,
    }

def main():
    _, run_timestamp, run_timestamp_iso = current_run_times()
    print(f"Run timestamp: {run_timestamp}")

    # POPルート（年→セット→カードURLを集めるため）
    pop_root = get_tcg_root_url()
    print(f"TCG root: {pop_root}")

    # 1) Set URLs（POP年ページ→セット→APRセットURLに変換）
    set_urls = list(iter_set_urls(pop_root))
    print(f"Sets found: {len(set_urls)}")

    # 2) Card URLs（セット→カードURL）
    card_urls = []
    for su in tqdm(set_urls, desc="Collecting cards from sets"):
        for cu in iter_card_urls_in_set(su):
            card_urls.append(cu)
            if DEBUG_LIMIT_CARDS is not None and len(card_urls) >= DEBUG_LIMIT_CARDS:
                break
        if DEBUG_LIMIT_CARDS is not None and len(card_urls) >= DEBUG_LIMIT_CARDS:
            break

    # 重複除去（順序保持）
    card_urls = list(dict.fromkeys(card_urls))
    print(f"Card URLs collected: {len(card_urls)}")

    # 3) Compute values
    all_values: list[CardValue] = []
    top10: list[CardValue] = []

    for cu in tqdm(card_urls, desc="Computing card values"):
        cv = parse_card_value(cu)
        if cv is None:
            continue

        all_values.append(cv)

        # Top10更新（都度ソートでOK、件数小さいので十分速い）
        top10.append(cv)
        top10.sort(key=lambda x: x.value_usd, reverse=True)
        top10 = top10[:10]

    # 指数計算用に全件を降順ソート
    all_values.sort(key=lambda x: x.value_usd, reverse=True)

    # 4) Top10 Output
    print("\n=== TOP 10 (by Grade10 AvgPrice*Pop) ===")
    for i, cv in enumerate(top10, 1):
        print(f"{i:2d}. ${cv.value_usd:,.2f}  {cv.name}  ({cv.url})")

    # 5) Pseudo Index (TopK sum rule)
    n = len(all_values)
    basket_size = 0
    sum_value = 0.0
    divisor_10000 = None
    index_level = None

    if n == 0:
        print("\n=== Pseudo Index Summary ===")
        print("Total cards valued: 0")
        print("Basket size used:  0")
        print("Basket sum value:  $0.00")
        print("Divisor (10,000 base): N/A")
        print("Index level (base 10,000): N/A")
    else:
        if n < 500:
            basket_size = max(1, n // 2)  # 上位半分（最低1）
        else:
            basket_size = 500

        basket = all_values[:basket_size]
        sum_value = sum(x.value_usd for x in basket)

        # 擬似指数: 10,000基準に正規化した divisor を併記
        divisor_10000 = (sum_value / 10000.0) if sum_value > 0 else None
        index_level = (sum_value / divisor_10000) if divisor_10000 else None  # = 10000

        print("\n=== Pseudo Index Summary ===")
        print(f"Total cards valued: {n}")
        print(f"Basket size used:  {basket_size}  (rule: n<500 => top half, else top500)")
        print(f"Basket sum value:  ${sum_value:,.2f}")
        if divisor_10000:
            print(f"Divisor (10,000 base): {divisor_10000:.6f}")
            print(f"Index level (base 10,000): {index_level:.2f}")
        else:
            print("Divisor (10,000 base): N/A")
            print("Index level (base 10,000): N/A")

    # 6) 保存系
    # 6-1) Index履歴を追記
    history_dir = Path("data")
    history_dir.mkdir(parents=True, exist_ok=True)
    history_path = history_dir / "index_history.csv"
    history_exists = history_path.exists()
    with history_path.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not history_exists:
            w.writerow(
                [
                    "run_timestamp_iso",
                    "run_timestamp_local",
                    "total_cards",
                    "basket_size",
                    "basket_sum_value_usd",
                    "divisor_10000",
                    "index_level",
                ]
            )
        w.writerow(
            [
                run_timestamp_iso,
                run_timestamp,
                n,
                basket_size,
                f"{sum_value:.2f}",
                f"{divisor_10000:.6f}" if divisor_10000 else "",
                f"{index_level:.2f}" if index_level else "",
            ]
        )

    # 6-2) Top10
    # Top10
    with open("top10.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["rank", "value_usd", "name", "url"])
        for i, cv in enumerate(top10, 1):
            w.writerow([i, f"{cv.value_usd:.2f}", cv.name, cv.url])

    # Basket（指数計算に使ったカード一覧）
    # ※再現性のため出すのがおすすめ
    if n > 0:
        with open("basket.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["rank", "value_usd", "name", "url"])
            for i, cv in enumerate(all_values[: (basket_size if n >= 1 else 0)], 1):
                w.writerow([i, f"{cv.value_usd:.2f}", cv.name, cv.url])

    with open("run_info.txt", "w", encoding="utf-8") as f:
        f.write(f"Run timestamp: {run_timestamp}\n")
        f.write(f"Run timestamp ISO: {run_timestamp_iso}\n")
        f.write(f"Total cards valued: {n}\n")
        f.write(f"Basket size used: {basket_size}\n")
        f.write(f"Basket sum value (USD): {sum_value:.2f}\n")
        f.write(
            f"Divisor (10,000 base): {divisor_10000:.6f}\n"
            if divisor_10000
            else "Divisor (10,000 base): N/A\n"
        )
        f.write(
            f"Index level (base 10,000): {index_level:.2f}\n"
            if index_level
            else "Index level (base 10,000): N/A\n"
        )

    print("\nSaved: top10.csv")
    if n > 0:
        print("Saved: basket.csv")
    print("Saved: run_info.txt")


if __name__ == "__main__":
    main()
