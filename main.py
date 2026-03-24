"""
SEBI & RBI Regulatory Circular Scraper — v7
--------------------------------------------
SEBI: direct requests to sebi.gov.in listing page
RBI:  direct requests to rbi.org.in (server-side rendered)
No Selenium required.

Run:              python main.py
SEBI only:        python main.py --source sebi
RBI only:         python main.py --source rbi
Skip summaries:   python main.py --no-summary
"""

import re
import time
import argparse
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from pypdf import PdfReader
from io import BytesIO
from supabase import create_client, Client

# ── CONFIGURATION ──────────────────────────────────────────────
import os
from dotenv import load_dotenv
load_dotenv()  # loads .env file if present, ignored in GitHub Actions

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
    "Connection": "keep-alive",
}

RBI_SKIP_TITLES = {
    "notifications", "master circulars", "draft notifications",
    "draft notifications/ guidelines", "draft notifications/guidelines",
    "index to rbi circulars", "circulars withdrawn", "press releases",
    "standalone circulars", "accessibility statement",
    "rbi's core purpose, values and vision", "media interactions",
    "memorial lectures", "external research schemes",
    "rbi occasional papers", "state statistics and finances",
    "public debt statistics", "list of returns", "data definition",
    "validation rules/ taxonomy", "list of rbi reporting portals",
    "faqs of rbi reporting portals", "master directions",
    "draft directions (re-wise)", "draft directions",
}

# ── SUPABASE ────────────────────────────────────────────────────
def get_supabase() -> Client:
    if "PASTE" in SUPABASE_URL:
        print("\n ERROR: Paste your Supabase credentials into main.py\n")
        exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# ── AI SUMMARY ──────────────────────────────────────────────────
def generate_summary(title: str, content: str) -> str:
    if "PASTE" in GROQ_API_KEY or not content or len(content) < 100:
        return ""
    try:
        response = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [
                    {"role": "system", "content": (
                        "You are a financial regulatory analyst. "
                        "Summarise this regulatory circular in 3-4 concise sentences. "
                        "Focus on: what changed, who is affected, and effective date. "
                        "Be factual and professional. No bullet points."
                    )},
                    {"role": "user", "content": f"Title: {title}\n\nContent:\n{content[:4000]}"}
                ],
                "max_tokens": 200, "temperature": 0.3
            },
            timeout=30
        )
        return response.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"    Warning: Summary failed: {e}")
        return ""

# ── PDF EXTRACTION ───────────────────────────────────────────────
def extract_pdf_text(url: str, session: requests.Session) -> str:
    try:
        r = session.get(url, timeout=30)
        r.raise_for_status()
        reader = PdfReader(BytesIO(r.content))
        text = "".join(p.extract_text() or "" for p in reader.pages)
        return re.sub(r'\s+', ' ', text).strip()
    except Exception as e:
        print(f"    Warning: PDF failed: {e}")
        return ""

# ── HTML EXTRACTION ──────────────────────────────────────────────
def extract_html_content(url: str, session: requests.Session) -> str:
    try:
        r = session.get(url, timeout=30)
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["nav", "header", "footer", "script", "style"]):
            tag.decompose()
        main = (
            soup.find("div", {"id": "mainContent"}) or
            soup.find("div", {"class": "content-area"}) or
            soup.find("td", {"class": "tabledata"}) or
            soup.find("article") or soup.find("main")
        )
        text = main.get_text(separator=" ", strip=True) if main \
               else soup.get_text(separator=" ", strip=True)
        return re.sub(r'\s+', ' ', text).strip()[:50000]
    except Exception as e:
        print(f"    Warning: HTML failed: {e}")
        return ""

# ── DATE EXTRACTION ──────────────────────────────────────────────
def extract_date_from_text(text: str):
    patterns_formats = [
        (r'(\d{1,2}[/-]\d{1,2}[/-]\d{4})', ["%d/%m/%Y", "%d-%m-%Y"]),
        (r'(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})', ["%d %B %Y"]),
        (r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})', ["%d %b %Y"]),
        (r'((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4})', ["%B %d, %Y", "%B %d %Y"]),
        (r'(\d{4}-\d{2}-\d{2})', ["%Y-%m-%d"]),
    ]
    for pattern, fmts in patterns_formats:
        match = re.search(pattern, text[:3000])
        if match:
            for fmt in fmts:
                try:
                    return datetime.strptime(match.group(1).strip(), fmt).date().isoformat()
                except:
                    continue
    return None

# ── CIRCULAR NUMBER ──────────────────────────────────────────────
def extract_circular_no(title: str, content: str, regulator: str) -> str:
    if regulator == "SEBI":
        patterns = [
            r'SEBI/[A-Za-z0-9/_()\-]+/CIR/\d{4}/\d+',
            r'SEBI/[A-Za-z0-9/_()\-]+/\d{4}/\d+',
            r'HO/[A-Za-z0-9/_()\-]+/\d{4}/\d+',
            r'CIR/[A-Z/]+/\d{4}/\d+',
        ]
    else:
        patterns = [
            r'RBI/\d{4}-\d{2,4}/\d+[A-Z()./\w]*',
            r'[A-Z]+\.[A-Z]+\.(?:REC|No|CIR)\.\w+/[\d.]+/\d{4}-\d{2,4}',
            r'RBI/\d{4}-\d{2,4}/\d+',
        ]
    for text in [title, content[:3000]]:
        for pattern in patterns:
            m = re.search(pattern, text)
            if m:
                return m.group()
    return ""

def is_real_rbi_circular(title: str) -> bool:
    title_lower = title.lower().strip()
    if title_lower in RBI_SKIP_TITLES or len(title) < 15:
        return False
    has_rbi_ref  = bool(re.search(r'RBI/\d{4}', title))
    has_dept_ref = bool(re.search(r'\b[A-Z]{2,}\.[A-Z]{2,}\.', title))
    has_year     = bool(re.search(r'20\d\d', title))
    is_news      = any(k in title_lower for k in [
        "imposes", "approves", "releases", "auction", "rate",
        "meeting", "statement", "result", "monetary", "penalty"
    ])
    return has_rbi_ref or has_dept_ref or (has_year and is_news)

def already_exists(supabase: Client, url: str) -> bool:
    try:
        return len(supabase.table("circulars").select("id").eq("url", url).execute().data) > 0
    except:
        return False

def store_circular(supabase: Client, data: dict) -> bool:
    try:
        if data.get("content"):
            data["content"] = data["content"].encode("utf-8", "ignore").decode("utf-8").replace("\x00", "")
        supabase.table("circulars").insert(data).execute()
        return True
    except Exception as e:
        print(f"    Failed to store: {e}")
        return False

# ════════════════════════════════════════════════════════════════
# SEBI SCRAPER
# ════════════════════════════════════════════════════════════════
def scrape_sebi(supabase: Client, max_circulars: int = 20, summarise: bool = True):
    print("\n Scraping SEBI circulars...")

    sources = [
        {"url": "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=7&smid=0", "category": "Circulars"},
        {"url": "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=6&smid=0", "category": "Master Circulars"}
    ]

    session = requests.Session()
    session.headers.update(HEADERS)
    count = 0

    for source in sources:
        if count >= max_circulars:
            break
        try:
            response = session.get(source["url"], timeout=30)
            print(f"    {source['category']}: HTTP {response.status_code} | {len(response.text)} chars")
            soup = BeautifulSoup(response.text, "html.parser")

            circular_links = [
                a for a in soup.find_all("a", href=True)
                if "/legal/circulars/" in a.get("href", "") or
                   "/legal/master-circulars/" in a.get("href", "")
            ]
            print(f"    {len(circular_links)} circular links found")

            for link in circular_links:
                if count >= max_circulars:
                    break

                href  = link["href"]
                title = link.get_text(strip=True)

                if not title or len(title) < 5:
                    continue
                if not href.startswith("http"):
                    href = "https://www.sebi.gov.in" + href
                if already_exists(supabase, href):
                    print(f"    Exists: {title[:55]}...")
                    continue

                print(f"    Fetching: {title[:65]}...")
                content = extract_pdf_text(href, session) if href.endswith(".pdf") \
                          else extract_html_content(href, session)

                # Date from URL e.g. /jan-2026/
                date_issued = None
                dm = re.search(r'/(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)-(\d{4})/', href, re.I)
                if dm:
                    try:
                        date_issued = datetime.strptime(f"01-{dm.group(1)}-{dm.group(2)}", "%d-%b-%Y").date().isoformat()
                    except: pass
                if not date_issued and content:
                    date_issued = extract_date_from_text(content)

                # Circular number — try Circular No. pattern first
                circular_no = ""
                cn_match = re.search(r'Circular No\.:\s*([\w/\-()\s]+?(?:/\d{4}|/I/[\d]+/\d{4}))', content)
                if cn_match:
                    circular_no = cn_match.group(1).strip()
                else:
                    circular_no = extract_circular_no(title, content, "SEBI")

                category = categorise_sebi(title)
                summary  = generate_summary(title, content) if summarise else ""

                print(f"    Content: {len(content)} chars | Ref: {circular_no or 'none'} | {date_issued or 'no date'}")

                data = {
                    "regulator": "SEBI", "title": title, "circular_no": circular_no,
                    "date_issued": date_issued, "category": category,
                    "url": href, "content": content[:50000], "summary": summary
                }
                if store_circular(supabase, data):
                    print(f"    Stored | {circular_no or 'no ref'} | {date_issued or 'no date'}")
                    count += 1
                time.sleep(1.5)

        except Exception as e:
            print(f"    SEBI error: {e}")

    print(f"\n  SEBI: {count} new circulars stored.")
    return count

def categorise_sebi(title: str) -> str:
    t = title.lower()
    if any(k in t for k in ["mutual fund", "amc", "scheme"]):             return "Mutual Funds"
    if any(k in t for k in ["insider trading", "takeover"]):              return "Takeover & Insider Trading"
    if any(k in t for k in ["broker", "stock broker", "trading member"]): return "Brokers & Trading"
    if any(k in t for k in ["listing", "ipo", "issue", "disclosure"]):    return "Listing & Disclosure"
    if any(k in t for k in ["portfolio manager", "pms"]):                 return "Portfolio Management"
    if any(k in t for k in ["derivative", "futures", "options"]):         return "Derivatives"
    if any(k in t for k in ["foreign portfolio", "fpi", "fii"]):          return "Foreign Portfolio Investors"
    if any(k in t for k in ["alternative investment", "aif"]):            return "Alternative Investment Funds"
    if any(k in t for k in ["cyber", "technology", "system"]):            return "Technology & Cybersecurity"
    return "General"

# ════════════════════════════════════════════════════════════════
# RBI SCRAPER
# ════════════════════════════════════════════════════════════════
def scrape_rbi(supabase: Client, max_circulars: int = 20, summarise: bool = True):
    print("\n Scraping RBI circulars...")

    sources = [
        {"url": "https://www.rbi.org.in/Scripts/BS_CircularIndexDisplay.aspx", "category": "Circulars"},
        {"url": "https://www.rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx",  "category": "Press Releases"}
    ]

    session = requests.Session()
    session.headers.update(HEADERS)
    count = 0

    for source in sources:
        if count >= max_circulars:
            break
        try:
            r    = session.get(source["url"], timeout=30)
            soup = BeautifulSoup(r.text, "html.parser")
            links = soup.find_all("a", href=True)
            print(f"    {len(links)} links on {source['category']} page")

            for link in links:
                if count >= max_circulars:
                    break

                href  = link["href"]
                title = link.get_text(strip=True)

                if not title or len(title) < 15:
                    continue
                if not any(k in href for k in ["Notification","circular","PressRelease","rdocs","notification","Circular"]):
                    continue
                if not is_real_rbi_circular(title):
                    continue

                if not href.startswith("http"):
                    href = "https://www.rbi.org.in/Scripts/" + href \
                           if not href.startswith("/") \
                           else "https://www.rbi.org.in" + href

                if already_exists(supabase, href):
                    print(f"    Exists: {title[:55]}...")
                    continue

                print(f"    Fetching: {title[:65]}...")
                content = extract_pdf_text(href, session) if href.endswith(".pdf") \
                          else extract_html_content(href, session)

                date_issued = extract_date_from_text(content) if content else None
                if not date_issued:
                    parent = link.find_parent("tr") or link.find_parent("td")
                    if parent:
                        date_issued = extract_date_from_text(parent.get_text())

                circular_no = extract_circular_no(title, content, "RBI")
                summary     = generate_summary(title, content) if summarise else ""

                print(f"    Content: {len(content)} chars | Ref: {circular_no or 'none'}")

                data = {
                    "regulator": "RBI", "title": title, "circular_no": circular_no,
                    "date_issued": date_issued, "category": categorise_rbi(title),
                    "url": href, "content": content[:50000], "summary": summary
                }
                if store_circular(supabase, data):
                    print(f"    Stored | {circular_no or 'no ref'} | {date_issued or 'no date'}")
                    count += 1
                time.sleep(1)

        except Exception as e:
            print(f"    RBI error: {e}")

    print(f"\n  RBI: {count} new circulars stored.")
    return count

def categorise_rbi(title: str) -> str:
    t = title.lower()
    if any(k in t for k in ["interest rate", "repo", "monetary policy"]): return "Monetary Policy"
    if any(k in t for k in ["bank", "banking", "nbfc", "lending"]):       return "Banking Regulation"
    if any(k in t for k in ["foreign exchange", "forex", "fema", "ecb"]): return "Foreign Exchange"
    if any(k in t for k in ["payment", "upi", "neft", "rtgs", "digital"]):return "Payments & Settlement"
    if any(k in t for k in ["capital", "crar", "tier", "basel"]):         return "Capital Adequacy"
    if any(k in t for k in ["priority sector", "agriculture", "msme"]):   return "Priority Sector Lending"
    if any(k in t for k in ["kyc", "aml", "anti-money", "fraud"]):        return "KYC & AML"
    if any(k in t for k in ["liquidity", "slr", "crr", "reserve"]):       return "Liquidity & Reserves"
    if any(k in t for k in ["cyber", "it", "technology", "data"]):        return "Technology & Cybersecurity"
    return "General"

# ════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="SEBI & RBI Circular Scraper")
    parser.add_argument("--source",     choices=["sebi", "rbi", "both"], default="both")
    parser.add_argument("--max",        type=int, default=20)
    parser.add_argument("--no-summary", action="store_true", help="Skip AI summaries")
    args = parser.parse_args()

    print("\n" + "="*60)
    print("  SEBI & RBI Regulatory Circular Scraper")
    print(f"  AI summaries: {'ON' if not args.no_summary else 'OFF'}")
    print("="*60)

    supabase = get_supabase()
    print("Connected to Supabase\n")

    total = 0
    if args.source in ("sebi", "both"):
        total += scrape_sebi(supabase, max_circulars=args.max, summarise=not args.no_summary)
    if args.source in ("rbi", "both"):
        total += scrape_rbi(supabase, max_circulars=args.max, summarise=not args.no_summary)

    print("\n" + "="*60)
    print(f"  Done. {total} new circulars added to database.")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()
