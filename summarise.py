"""
One-Time Summary Generator
--------------------------
Generates AI summaries for existing circulars rows that have
content but no summary yet. Run once after initial scrape.

Run: python summarise.py
RBI only: python summarise.py --source rbi
SEBI only: python summarise.py --source sebi
"""

import os
import time
import argparse
import requests
from supabase import create_client, Client

# ── CONFIGURATION ──────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL","")
SUPABASE_KEY = os.getenv("SUPABASE_KEY","")
GROQ_API_KEY = os.getenv("GROQ_API_KEY","")

# ── SUPABASE ────────────────────────────────────────────────────
def get_supabase() -> Client:
    if "PASTE" in SUPABASE_URL:
        print("\n ERROR: Paste your Supabase credentials\n")
        exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# ── AI SUMMARY ──────────────────────────────────────────────────
def generate_summary(title: str, content: str) -> str:
    if not content or len(content) < 100:
        return ""
    try:
        response = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json"
            },
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
                "max_tokens": 200,
                "temperature": 0.3
            },
            timeout=30
        )
        return response.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"    Warning: Groq failed: {e}")
        return ""

# ── MAIN ────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Generate summaries for existing rows")
    parser.add_argument("--source", choices=["sebi", "rbi", "both"], default="both")
    args = parser.parse_args()

    print("\n" + "="*60)
    print("  One-Time Summary Generator")
    print("="*60)

    supabase = get_supabase()
    print("Connected to Supabase\n")

    # Fetch rows that have content but no summary
    query = supabase.table("circulars").select("id, regulator, title, content, summary")

    if args.source != "both":
        query = query.eq("regulator", args.source.upper())

    result = query.execute()
    rows   = result.data

    # Filter to rows with content but empty/missing summary
    to_update = [
        r for r in rows
        if r.get("content") and len(r["content"]) > 100
        and not r.get("summary")
    ]

    print(f"Rows with content: {len([r for r in rows if r.get('content') and len(r['content']) > 100])}")
    print(f"Rows needing summary: {len(to_update)}\n")

    if not to_update:
        print("Nothing to update — all rows with content already have summaries.")
        return

    updated = 0
    skipped = 0

    for row in to_update:
        title   = row["title"]
        content = row["content"]
        row_id  = row["id"]
        reg     = row["regulator"]

        print(f"  [{reg}] {title[:60]}...")
        summary = generate_summary(title, content)

        if summary:
            try:
                supabase.table("circulars").update(
                    {"summary": summary}
                ).eq("id", row_id).execute()

                # Verify the update actually saved
                verify = supabase.table("circulars").select("summary").eq("id", row_id).execute()
                saved  = verify.data[0].get("summary", "") if verify.data else ""
                if saved and len(saved) > 10:
                    print(f"    Summary saved and verified ({len(saved)} chars)")
                    updated += 1
                else:
                    print(f"    WARNING: Update ran but summary is still empty!")
                    skipped += 1
            except Exception as e:
                print(f"    Failed to save: {e}")
                skipped += 1
        else:
            print(f"    Skipped — content too short or Groq unavailable")
            skipped += 1

        time.sleep(0.5)  # respect Groq rate limits

    print(f"\n{'='*60}")
    print(f"  Done. {updated} summaries generated, {skipped} skipped.")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()