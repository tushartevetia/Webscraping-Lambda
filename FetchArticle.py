import feedparser
from datetime import datetime, timedelta
import json
import logging
import time
import re
import boto3
from urllib.parse import unquote
# Configure logging
logging.getLogger().setLevel(logging.INFO)
s3 = boto3.client('s3')
def extract_real_url(google_url):
   """
   Extract the actual URL from Google News URL
   """
   try:
       url_part = google_url.split('url=')[-1]
       return unquote(url_part)
   except Exception:
       return google_url
def lambda_handler(event, context):
   try:
       # Load prompts from prompts.json
       with open('prompts.json', 'r') as f:
           prompts = json.load(f)
      # Check if prompts is a list
       if not isinstance(prompts, list):
           prompts = [prompts]

        # Generate the current date and time for the folder name
       current_datetime = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
       folder_name = f"scraping_results_{current_datetime}/"
        # Process each query
       for prompt in prompts:
           query = prompt.get('query')
           query = query.replace(' ','%20')
           if not query:
               raise ValueError("No query found in prompts.json")

           # Construct RSS feed URL
           rss_url = f"https://news.google.com/rss/search?q={query}"

           # Parse RSS feed
           feed = feedparser.parse(rss_url)
           if not feed.entries:
               logging.info(f"No articles found for query: {query}")
               continue
       # Process articles from the past 24 hours
           articles = []
           for entry in feed.entries:
               try:
                   published_date = datetime.strptime(entry.published, "%a, %d %b %Y %H:%M:%S %Z")
                   if published_date > datetime.now() - timedelta(hours=24):
                       article = {
                           'title': entry.title,
                           'link': extract_real_url(entry.link),
                           'published_date': entry.published,
                           'source': entry.get('source', {}).get('title', 'Unknown'),
                           'content': None  # Placeholder for content to be filled by second lambda
                       }
                       articles.append(article)
               except Exception as e:
                   logging.error(f"Error processing feed entry: {e}")
           # Sort articles by date
           sorted_news = sorted(
               articles,
               key=lambda x: datetime.strptime(x["published_date"], "%a, %d %b %Y %H:%M:%S %Z"),
               reverse=True
           )
           # Generate filename and save to S3
           query = query.replace('%20',' ')
           safe_query = re.sub(r'[^\w\-_\. ]', '_', query)
           json_filename = f'news_results_{safe_query}_{time.strftime("%Y-%m-%d_%H-%M-%S")}.json'
           s3_scrape_results=folder_name+json_filename
           json_data = json.dumps(sorted_news, indent=4)
           s3.put_object(
               Body=json_data,
               Bucket='<your-bucket-name>',
               Key=s3_scrape_results,
               ContentType='application/json'
           )
           # Invoke the second Lambda function
           lambda_client = boto3.client('lambda')
           lambda_client.invoke(
               FunctionName='Webscrapper',
               InvocationType='Event',  # Asynchronous invocation
               Payload=json.dumps({
                   'bucket': '<your-bucket-name>',
                   'file_key': s3_scrape_results
               })
           )
           logging.info(f"Processed query: {query}")
       return {
           'statusCode': 200,
           'body': json.dumps({
               'message': 'RSS feed fetching completed successfully',
           })
       }
   except Exception as e:
       logging.error(f"Lambda execution failed: {e}")
       return {
           'statusCode': 500,
           'body': json.dumps({
               'error': str(e),
           })
       }