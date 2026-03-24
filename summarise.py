"""
One-Time Summary Generator
--------------------------
Generates AI summaries for existing circulars rows that have
content but no summary yet. Run once after initial scrape.

Run: python summarise.py
RBI only: python summarise.py --source rbi
SEBI only: python summarise.py --source sebi
"""

import time
import argparse
import requests
from supabase import create_client, Client

# ── CONFIGURATION ──────────────────────────────────────────────
SUPABASE_URL = "PASTE_YOUR_SUPABASE_URL"
SUPABASE_KEY = "PASTE_YOUR_SUPABASE_KEY"
GROQ_API_KEY = "PASTE_YOUR_GROQ_API_KEY"

# ── SUPABASE ────────────────────────────────────────────────────
def get_supabase() -> Client:
    if "PASTE" in SUPABASE_URL:
        print("\n ERROR: Paste your Supabase credentials\n")
        exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# ── AI SUMMARY ──────────────────────────────────────────────────
def generate_summary(title: str, content: str, regulator: str) -> str:
    """
    RBI — full AI summary from content.
    SEBI — structured placeholder from title/metadata (content too thin).
    """
    if regulator == "SEBI":
        # Generate a structured placeholder from title alone
        return (
            f"This SEBI circular titled '{title}' is available in the database with "
            f"metadata including circular reference, date, and category. Full body text "
            f"is pending extraction due to SEBI website restrictions. Please verify the "
            f"complete circular directly at sebi.gov.in for compliance purposes."
        )

    # RBI — full AI summary
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

    # Filter to rows needing summary — RBI needs content, SEBI just needs title
    to_update = [
        r for r in rows
        if not r.get("summary") and (
            r["regulator"] == "SEBI" or  # SEBI always gets placeholder
            (r.get("content") and len(r["content"]) > 100)  # RBI needs content
        )
    ]

    sebi_count = len([r for r in to_update if r["regulator"] == "SEBI"])
    rbi_count  = len([r for r in to_update if r["regulator"] == "RBI"])

    print(f"Rows with content (RBI): {len([r for r in rows if r.get('content') and len(r['content']) > 100])}")
    print(f"Rows needing summary — RBI: {rbi_count} | SEBI: {sebi_count}\n")

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
        summary = generate_summary(title, content, reg)

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

        time.sleep(0.5 if reg == "RBI" else 0)  # only RBI calls Groq API

    print(f"\n{'='*60}")
    print(f"  Done. {updated} summaries generated, {skipped} skipped.")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
