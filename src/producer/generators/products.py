from uuid import uuid4
from datetime import datetime, UTC
from random import randint, choices, shuffle

CATEGORIES = [
    "electronics",
    "fashion",
    "home",
    "sports",
    "books",
    "beauty",
    "toys",
    "grocery",
    "automotive",
    "health",
    "jewelry",
    "furniture",
    "garden",
    "pet_supplies",
    "office",
    "gaming",
    "baby",
    "music",
    "movies",
    "shoes",
]

CATEGORY_WEIGHTS = [
    1,   # shuffled
    2,   
    3,    
    4,    
    1,   
    9,    
    1,   
    13,    
    4,   
    5,    
    3,    
    2,    
    99,   
    6,    
    50,   
    26,    
    48,   
    8,    
    30,   
    47,   
]

def generate_product_viewed():
    return {
        "event_id": str(uuid4()),
        "user_id": randint(1, 100000),
        "product_id": randint(1, 10000),
        "category": choices(
            CATEGORIES,
            weights=CATEGORY_WEIGHTS,
            k=1
        )[0],
        "event_timestamp": datetime.now(UTC).isoformat()
    }