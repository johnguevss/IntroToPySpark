import tweepy

# Twitter API credentials
consumer_key = "YOUR_CONSUMER_KEY"
consumer_secret = "YOUR_CONSUMER_SECRET"
access_token = "YOUR_ACCESS_TOKEN"
access_token_secret = "YOUR_ACCESS_TOKEN_SECRET"

# Authenticate with Twitter API
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

# Create API object
api = tweepy.API(auth)

# Hashtag to search
hashtag = "#example"

# Number of tweets to retrieve
num_tweets = 10

# Retrieve tweets
tweets = tweepy.Cursor(api.search, q=hashtag, tweet_mode='extended').items(num_tweets)

# Process and print each tweet
for tweet in tweets:
    print("Username:", tweet.user.screen_name)
    print("Tweet:", tweet.full_text)
    print("--------")
