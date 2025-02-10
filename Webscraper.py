from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from tempfile import mkdtemp
import json
import time
import random
import logging
import boto3
from bs4 import BeautifulSoup
from unidecode import unidecode
# Configure logging
logging.getLogger().setLevel(logging.INFO)
USER_AGENTS = [
   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36",
   "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36",
   "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36"
]
s3 = boto3.client('s3')
def get_articles_with_progress(bucket, file_key):
   """
   Get articles and determine the next unprocessed index
   """
   try:
       response = s3.get_object(Bucket=bucket, Key=file_key)
       articles = json.loads(response['Body'].read().decode('utf-8'))
       # Find the first article without content
       start_index = 0
       start_index = next((i for i, article in enumerate(articles) if 'content' not in article or (not article['content'] and 'failed' not in article)),len(articles))
       return articles, start_index
   except Exception as e:
       logging.error(f"Error reading articles: {e}")
       raise
def save_progress(bucket, file_key, articles):
   """
   Save the current progress of articles to S3
   """
   try:
       s3.put_object(
           Body=json.dumps(articles, indent=4),
           Bucket=bucket,
           Key=file_key,
           ContentType='application/json',
           CacheControl='no-cache, no-store, must-revalidate'
       )
       logging.info("Progress saved successfully")
       return True
   except Exception as e:
       logging.error(f"Error saving progress: {e}")
       return False

def configure_chrome_options():
   """Configure Chrome options with optimized settings for Lambda"""
   chrome_options = Options()
   chrome_options.binary_location = '/opt/headless-chromium'
   chrome_options.add_argument('--headless')
   chrome_options.add_argument('--no-sandbox')
   chrome_options.add_argument("--disable-gpu")
   chrome_options.add_argument("--window-size=1920x1080")
   chrome_options.add_argument("--start-maximized")
   chrome_options.add_argument("--single-process")
   chrome_options.add_argument("--disable-dev-shm-usage")
   chrome_options.add_argument("--disable-dev-tools")
   chrome_options.add_argument("--no-zygote")
   chrome_options.add_argument(f"--user-data-dir={mkdtemp()}")
   chrome_options.add_argument(f"--data-path={mkdtemp()}")
   chrome_options.add_argument(f"--disk-cache-dir={mkdtemp()}")
   chrome_options.add_argument("--remote-debugging-port=9222")
   chrome_options.add_argument('--disable-web-security')
   chrome_options.add_argument('--dns-prefetch-disable')
   chrome_options.add_argument('--disable-infobars')
   chrome_options.add_argument('--disable-extensions')
   chrome_options.add_argument(f'user-agent={random.choice(USER_AGENTS)}')
   chrome_options.add_experimental_option("prefs",{"profile.managed_default_content_settings.images":2})
   return chrome_options

def clean_text(text):
   """Clean and normalize text content"""
   if not text:
       return ""
   text = text.strip()
   text = text.replace('\n', ' ')
   text = text.replace('\t', ' ')
   text = unidecode(text)
   return text

def extract_text_content(soup):
   """Extract all meaningful text content from the page"""
   text = soup.get_text()
   return clean_text(text)

def create_driver():
   """Create and configure Chrome WebDriver with retry mechanism"""
   max_retries = 3
   for attempt in range(max_retries):
       try:
           chrome_service = Service("/opt/chromedriver")
           chrome_options = configure_chrome_options()
           driver = webdriver.Chrome(service=chrome_service, options=chrome_options)
           driver.set_page_load_timeout(30)
           return driver
       except Exception as e:
           logging.error(f"Attempt {attempt + 1} failed to create WebDriver: {e}")
           if attempt == max_retries - 1:
               raise
           time.sleep(1)

def wait_for_page_load(driver, timeout=30):
   """Wait for page load with enhanced conditions"""
   try:
       WebDriverWait(driver, timeout).until(
           lambda d: d.execute_script('return document.readyState') == 'complete'
       )
       time.sleep(3)
       driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
       time.sleep(2)
       driver.execute_script("window.scrollTo(0, 0);")
       time.sleep(1)
       return True
   except Exception as e:
       logging.error(f"Page load wait error: {e}")
       return False
   
def scrape_with_retry(driver, url, max_retries=3):
   """Scrape content with retry mechanism"""
   for attempt in range(max_retries):
       try:
           driver.delete_all_cookies()
           driver.get(url)
           if not wait_for_page_load(driver):
               raise TimeoutException("Page load wait timeout")
           page_source = driver.page_source
           soup = BeautifulSoup(page_source, 'html.parser')
           content = extract_text_content(soup)
           if content:
               return f"{content}"
       except TimeoutException as e:
           logging.warning(f"Timeout on attempt {attempt + 1}: {str(e)}")
           if attempt == max_retries - 1:
               return f"Error: Failed to load page after {max_retries} attempts"
       except Exception as e:
           logging.error(f"Error on attempt {attempt + 1}: {str(e)}")
           if attempt == max_retries - 1:
               return f"Error: {str(e)}"
       time.sleep((attempt + 1) * 2)
   return "Error: Failed to scrape content"

def lambda_handler(event, context):
   driver = None
   try:
       # Get S3 bucket and file information from the event
       bucket = event['bucket']
       file_key = event['file_key']
       logging.info(f"Query Logs for the file: {file_key}")
       # Get articles and determine starting point
       articles, start_index = get_articles_with_progress(bucket, file_key)
       # Check if we've already processed all articles
       if start_index >= len(articles):
           return {
               'statusCode': 200,
               'body': json.dumps({
                   'message': 'All articles have been processed',
                   'total_articles': len(articles)
               })
           }
       logging.info(f"Starting processing from index {start_index}")
       driver = create_driver()
       if not driver:
           raise Exception("Failed to initialize WebDriver")
       # Calculate time thresholds
       start_time = time.time()
       lambda_timeout = 15 * 60  # 15 minutes in seconds
       save_threshold = lambda_timeout - 60  # Save 1 minute before timeout
       try:
           for index in range(start_index, len(articles)):
               # Check if we're approaching the timeout
               elapsed_time = time.time() - start_time
               if elapsed_time >= save_threshold:
                   logging.warning("Approaching Lambda timeout, saving progress and exiting")
                   # Save current progress
                   save_progress(bucket, file_key, articles)
               try:
                   logging.info(f"Scraping article {index + 1}: {articles[index]['title']}")
                   content = scrape_with_retry(driver, articles[index]['link'])
                #    If page load failed remove article
                   if content.startswith("Error:"):
                       logging.warning(f"Removing article {index+1} due to page load failure")
                       articles[index]['failed'] = True
                   else:
                        articles[index]['content'] = content
                   # Save progress every 5 articles to handle potential crashes
                   if (index + 1) % 5 == 0:
                       save_progress(bucket, file_key, articles)
                   time.sleep(random.uniform(3, 5))
               except Exception as e:
                   logging.error(f"Error processing article {index + 1}: {e}")
                   articles[index]['content'] = f"Error: {str(e)}"
                   # Recreate driver if it fails
                   try:
                       driver.quit()
                   except Exception:
                       pass
                   driver = create_driver()
                   if not driver:
                       raise Exception("Failed to reinitialize WebDriver")
       finally:
           if driver:
               try:
                   driver.quit()
               except Exception as e:
                   logging.error(f"Error closing driver: {e}")
       # Save final progress
       save_progress(bucket, file_key, articles)
       return {
           'statusCode': 200,
           'body': json.dumps({
               'message': 'Scraping completed successfully',
               'json_file': file_key,
               'total_articles': len(articles),
               'articles_with_content': sum(1 for article in articles if article['content'] and not article['content'].startswith('Error'))
           })
       }
   except Exception as e:
       logging.error(f"Lambda execution failed: {e}")
       return {
           'statusCode': 500,
           'body': json.dumps({
               'error': str(e),
               # Return the same event structure for auto-reinvocation
               'bucket': bucket,
               'file_key': file_key
           })
       }
