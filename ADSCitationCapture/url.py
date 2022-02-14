import os
import requests
import urllib.request, urllib.parse, urllib.error
import re
from adsputils import setup_logging

# ============================= INITIALIZATION ==================================== #
# - Use app logger:
#import logging
#logger = logging.getLogger('ads-citation-capture')
# - Or individual logger for this file:
from adsputils import setup_logging, load_config
proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
config = load_config(proj_home=proj_home)
logger = setup_logging(__name__, proj_home=proj_home,
                        level=config.get('LOGGING_LEVEL', 'INFO'),
                        attach_stdout=config.get('LOG_STDOUT', False))

url_regex = re.compile(
        r'^(?:http)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)


# =============================== FUNCTIONS ======================================= #
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

def is_github(url):
    try:
        domain = urllib.parse.urlparse(url).hostname

    except Exception as e:
        msg = "Failed to verify {}".format(url)
        logger.exception(msg)
        raise
    
    return True if domain in ["github.com", "www.github.com", "gist.github.com"] else False

def is_gist(url):
    try:
        domain = urllib.parse.urlparse(url).hostname

    except Exception as e:
        msg = "Failed to verify {}".format(url)
        logger.exception(msg)
        raise
    
    return True if domain in ["gist.github.com"] else False
    
