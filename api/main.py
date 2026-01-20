from fastapi import FastAPI, Depends, Request, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from .database import get_db
import logging
import time
import os

# Ensure the logs directory exists
if not os.path.exists("logs"):
    os.makedirs("logs")

# SETUP STRUCTURED LOGGING
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/api_access.log"), # Permanent file record
        logging.StreamHandler()                     # Real-time terminal output
    ]
)
logger = logging.getLogger("MedicalAPI")

app = FastAPI(title="Pharmaceutical Sales API")

# LOGGING MIDDLEWARE (The Flight Recorder)
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    
    log_message = "Method: {} Path: {} Status: {} Duration: {:.2f}s".format(
        request.method, 
        request.url.path, 
        response.status_code, 
        process_time
    )
    logger.info(log_message)
    return response

@app.get("/")
def read_root():
    return {"message": "Welcome to the Medical Analytical API"}

# --- REQUESTED ENDPOINT ---

@app.get("/api/reports/top-products")
def get_top_products(
    limit: int = Query(10, description="Number of top products to return"), 
    db: Session = Depends(get_db)
):
    """
    Returns the most frequently mentioned terms/products across all channels.
    Example: /api/reports/top-products?limit=10
    """
    logger.info(f"Generating Top Products report (limit={limit})")
    
    # We query the fct_messages table in the 'main' schema
    # We group by message_text and count occurrences
    sql = text("""
        SELECT 
    message_text AS term, 
    COUNT(*) AS mention_count
    FROM raw.fct_messages
    WHERE message_text IS NOT NULL  -- Exclude empty messages
    GROUP BY message_text
    ORDER BY mention_count DESC
    LIMIT :limit
    """)
    
    # Execute and return as list of dictionaries using mappings()
    result = db.execute(sql, {"limit": limit}).mappings().all()
    return result

# --- ENDPOINT 2: CHANNEL ACTIVITY (The one you just asked for) ---
@app.get("/api/channels/{channel_name}/activity")
def get_channel_activity(channel_name: str, db: Session = Depends(get_db)):
    """
    Returns daily posting activity and trends for a specific channel.
    Handles the conversion of integer date_key to a proper Date type.
    """
    logger.info(f"Retrieving activity trends for channel: {channel_name}")
    
    # We use TO_DATE because date_key is an integer (e.g., 20260120)
    sql = text("""
        SELECT 
            TO_DATE(m.date_key::TEXT, 'YYYYMMDD') AS day, 
            COUNT(m.message_id) AS post_count,
            SUM(m.view_count) AS daily_views
        FROM "raw".fct_messages m
        JOIN "raw".dim_channels c ON m.channel_key = c.channel_key
        WHERE c.channel_name = :channel_name
        GROUP BY day
        ORDER BY day DESC
    """)
    
    result = db.execute(sql, {"channel_name": channel_name}).mappings().all()
    
    if not result:
        raise HTTPException(status_code=404, detail=f"No activity found for channel: {channel_name}")
        
    return result

# --- ENDPOINT 3: MESSAGE SEARCH ---
@app.get("/api/search/messages")
def search_messages(
    query: str = Query(..., description="The keyword to search for"),
    limit: int = Query(20, description="Maximum number of results to return"),
    db: Session = Depends(get_db)
):
    """
    Searches for messages containing a specific keyword (case-insensitive).
    Example: /api/search/messages?query=paracetamol&limit=20
    """
    logger.info(f"Searching messages for keyword: {query} with limit: {limit}")
    
    # We use ILIKE for case-insensitive matching and % wildcards for "contains" search
    sql = text("""
        SELECT 
            message_id,
            TO_DATE(date_key::TEXT, 'YYYYMMDD') AS date,
            message_text,
            view_count
        FROM "raw".fct_messages
        WHERE message_text ILIKE :search_term
        ORDER BY date_key DESC
        LIMIT :limit
    """)
    
    # We wrap the query in % symbols so it finds the word anywhere in the sentence
    params = {
        "search_term": f"%{query}%",
        "limit": limit
    }
    
    result = db.execute(sql, params).mappings().all()
    
    if not result:
        logger.info(f"No messages found for query: {query}")
        return []
        
    return result

# --- ENDPOINT 4: VISUAL CONTENT STATS ---
@app.get("/api/reports/visual-content")
def get_visual_stats(db: Session = Depends(get_db)):
    """
    Returns statistics about image usage and YOLO detection categories across channels.
    """
    logger.info("Generating Visual Content Statistics report")
    
    # This query calculates total images per channel and identifying the most frequent category
    # Note: We assume you have a table named fct_image_detections from your Task 3
    sql = text("""
        SELECT 
            c.channel_name,
            COUNT(i.message_id) AS total_images,
            MODE() WITHIN GROUP (ORDER BY i.detected_class) AS primary_category,
            ROUND(AVG(i.confidence_score)::numeric, 2) AS avg_confidence
        FROM "raw".fct_image_detections i
        JOIN "raw".dim_channels c ON i.channel_key = c.channel_key
        GROUP BY c.channel_name
        ORDER BY total_images DESC
    """)
    
    try:
        result = db.execute(sql).mappings().all()
        return result
    except Exception as e:
        logger.error(f"Error retrieving visual stats: {e}")
        raise HTTPException(status_code=500, detail="Visual detection data not available")