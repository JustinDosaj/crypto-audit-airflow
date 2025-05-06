import random
from datetime import datetime, timedelta

def mock_fetch_tweets(handle, count = 50):
    
    posts = []


    for i in range(count):

        post = {
            "username": handle,
            "tweet_id": f"{handle}_tweet{i}",
            "created_at": (datetime.now() - timedelta(days=random.randint(0, 30))).isoformat() # Choose random day within the past 30 days to mock the creation time of post
        }

        # Every third tweet will contain a crypto ticker—this indicates a promotion
        # Every third tweet is promoting $GIGA
        # Every fifth tweet is promoting $BTC if not divisible by 3
        # Remaining posts contain no promotions
        if(i % 3 == 0):
            post["text"] = f"Mock Tweet from {handle} promoting $GIGA"
        elif(i % 5 == 0):
            post["text"] = f"Mock Tweet from {handle} promoting $BTC"
        else:
            post["text"] = f"Mock Tweet from {handle} not promoting anything"
        
        posts.append(post)
    
    return posts