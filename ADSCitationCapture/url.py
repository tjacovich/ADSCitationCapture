import requests
import re
from adsputils import setup_logging

logger = setup_logging(__name__)
#logger.propagate = False

url_regex = re.compile(
        r'^(?:http)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

def is_url(text):
    return True if url_regex.search(text) else False

def is_alive(url):
    if is_url(url):
        try:
            request = requests.get(url)
        except:
            logger.exception("Failed URL: %s", url)
            raise
        return request.ok
    return False
