import json
import os
import psycopg2
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ─── CONFIG ──────────────────────────────────────────────────────────────────
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "medical_warehouse")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Absolute path to your raw data
DATA_ROOT = Path(r"C:\Users\hanif\Desktop\10Academy\Shipping-a-Data-Product-From-Raw-Telegram-Data-to-an-Analytical-API\data\raw\telegram_messages")

# ─── CONNECTION ──────────────────────────────────────────────────────────────
def connect_db():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def main():
    conn = connect_db()
    cur = conn.cursor()

    # Create schema & table if not exists
    cur.execute("""
    CREATE SCHEMA IF NOT EXISTS raw;

    CREATE TABLE IF NOT EXISTS raw.telegram_messages (
        message_id        BIGINT,
        channel_username  TEXT,
        channel_title     TEXT,
        date              TIMESTAMP WITH TIME ZONE,
        text              TEXT,
        views             INTEGER,
        forwards          INTEGER,
        has_media         BOOLEAN,
        image_path        TEXT,
        loaded_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (message_id, channel_username)
    );
    """)
    conn.commit()

    inserted = 0
    skipped = 0

    # ─── LOAD ALL JSON FILES ─────────────────────────────────────────────────
    # rglob("*.json") scans all subfolders for .json files
    for file_path in DATA_ROOT.rglob("*.json"):
        if "_manifest.json" in file_path.name:
            continue

        print(f"Loading {file_path.name} ...")

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                
                # Handle both list of objects and single object
                messages = data if isinstance(data, list) else [data]

                for msg in messages:
                    try:
                        # MAPPING: JSON keys -> Python variables -> SQL columns
                        # Based on your JSON example provided
                        m_id = msg.get("message_id")
                        c_username = msg.get("channel_name")  # In your JSON it's channel_name
                        c_title = msg.get("channel_title")
                        m_date = msg.get("message_date")      # In your JSON it's message_date
                        m_text = msg.get("message_text")      # In your JSON it's message_text
                        m_views = msg.get("views") or 0
                        m_forwards = msg.get("forwards") or 0
                        m_has_media = msg.get("has_media") or False
                        m_image = msg.get("image_path")

                        if m_id is None or c_username is None:
                            continue

                        cur.execute("""
                        INSERT INTO raw.telegram_messages (
                            message_id, channel_username, channel_title, date,
                            text, views, forwards, has_media, image_path
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (message_id, channel_username) DO NOTHING;
                        """, (
                            m_id, c_username, c_title, m_date,
                            m_text, m_views, m_forwards, m_has_media, m_image
                        ))

                        if cur.rowcount > 0:
                            inserted += 1
                        else:
                            skipped += 1 # Duplicate record
                            
                    except Exception as e:
                        print(f"  Error record {msg.get('message_id')}: {e}")
                        skipped += 1

            # Commit after each file is processed
            conn.commit()

        except json.JSONDecodeError:
            print(f"  Skipping {file_path.name}: Invalid JSON format.")
        except Exception as e:
            print(f"  Error processing file {file_path.name}: {e}")

    cur.close()
    conn.close()

    print("-" * 30)
    print(f"Done! Successfully Inserted: {inserted:,}")
    print(f"Duplicates or Errors skipped: {skipped:,}")
    print("-" * 30)

if __name__ == "__main__":
    main()