
import os
import requests
import urllib.request, urllib.parse, urllib.error
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


# =============================== FUNCTIONS ======================================= #
def _request_citations_page(app, bibcode, start, rows):
    params = urllib.parse.urlencode({
                'fl': 'bibcode',
                'q': 'citations(bibcode:{0})'.format(bibcode),
                'start': start,
                'rows': rows,
                'sort': 'date desc, bibcode desc',
            })
    headers = {}
    headers["Authorization"] = "Bearer:{}".format(app.conf['ADS_API_TOKEN'])
    url = app.conf['ADS_API_URL']+"search/query?"+params
    r_json = {}
    try:
        r = requests.get(url, headers=headers)
    except:
        logger.error("Search API request failed for citations (start: %i): %s", start, bibcode)
        raise
    if not r.ok:
        msg = "Search API request with error code '{}' for bibcode (start: {}): {}".format(r.status_code, start, bibcode)
        logger.error(msg)
        raise Exception(msg)
    else:
        try:
            r_json = r.json()
        except ValueError:
            msg = "No JSON object could be decoded from Search API response when searching canonical bibcodes (start: {}) for: {}".format(start, bibcode)
            logger.error(msg)
            raise Exception(msg)
        else:
            return r_json
    return r_json

def request_existing_citations(app, bibcode):
    start = 0
    rows = 25
    existing_citation_bibcodes = []
    n_existing_citations = None
    while True:
        retries = 0
        while True:
            try:
                answer = _request_citations_page(app, bibcode, start, rows)
            except:
                if retries < 3:
                    logger.info("Retrying Search API request for citations (start: %i): %s", start, bibcode)
                    retries += 1
                else:
                    logger.exception("Failed Search API request for citations (start: %i): %s", start, bibcode)
                    raise
            else:
                break
        existing_citation_bibcodes += answer['response']['docs']
        if n_existing_citations is None:
            n_existing_citations = answer['response']['numFound']
        start += rows
        if start > int(n_existing_citations):
            break
    # Transform from list of dict to list of bibcodes:
    existing_citation_bibcodes = [b['bibcode'] for b in existing_citation_bibcodes]
    return existing_citation_bibcodes

def get_canonical_bibcodes(app, bibcodes, timeout=30):
    """
    Convert input bibcodes into their canonical form if they exist, hence
    the returned list can be smaller than the input bibcode list
    """
    chunk_size = 2000 # Max number of records supported by bigquery
    bibcodes_chunks = [bibcodes[i * chunk_size:(i + 1) * chunk_size] for i in range((len(bibcodes) + chunk_size - 1) / chunk_size )]
    canonical_bibcodes = []
    total_n_chunks = len(bibcodes_chunks)
    # Execute multiple requests to bigquery if the list of bibcodes is longer than the accepted maximum
    for n_chunk, bibcodes_chunk in enumerate(bibcodes_chunks):
        retries = 0
        while True:
            try:
                canonical_bibcodes += _get_canonical_bibcodes(app, n_chunk, total_n_chunks, bibcodes_chunk, timeout)
            except:
                if retries < 3:
                    logger.info("Retrying BigQuery API request for bibcodes (chunk: %i/%i): %s", n_chunk+1, total_n_chunks, " ".join(bibcodes_chunk))
                    retries += 1
                else:
                    logger.exception("Failed BigQuery API request for bibcodes (chunk: %i/%i): %s", n_chunk+1, total_n_chunks, " ".join(bibcodes_chunk))
                    raise
            else:
                break
    return canonical_bibcodes

def _get_canonical_bibcodes(app, n_chunk, total_n_chunks, bibcodes_chunk, timeout):
    canonical_bibcodes = []
    params = urllib.parse.urlencode({
                'fl': 'bibcode',
                'q': '*:*',
                'wt': 'json',
                'fq':'{!bitset}',
                'rows': len(bibcodes_chunk),
            })
    headers = {}
    headers["Authorization"] = "Bearer:{}".format(app.conf['ADS_API_TOKEN'])
    headers["Content-Type"] = "big-query/csv"
    url = app.conf['ADS_API_URL']+"search/bigquery?"+params
    r_json = {}
    data = "bibcode\n" + "\n".join(bibcodes_chunk)
    try:
        r = requests.post(url, headers=headers, data=data, timeout=timeout)
    except:
        logger.error("BigQuery API request failed for bibcodes (chunk: %i/%i): %s", n_chunk+1, total_n_chunks, " ".join(bibcodes_chunk))
        raise
    if not r.ok:
        msg = "BigQuery API request with error code '{}' for bibcodes (chunk: {}/{}): {}".format(r.status_code, n_chunk+1, total_n_chunks, " ".join(bibcodes_chunk))
        logger.error(msg)
        raise Exception(msg)
    else:
        try:
            r_json = r.json()
        except ValueError:
            msg = "No JSON object could be decoded from BigQuery API response when searching canonical bibcodes (chunk: {}/{}) for: {}".format(n_chunk+1, total_n_chunks, " ".join(bibcodes_chunk))
            logger.error(msg)
            raise Exception(msg)
        else:
            for paper in r_json.get('response', {}).get('docs', []):
                canonical_bibcodes.append(paper['bibcode'])
    return canonical_bibcodes

def get_canonical_bibcode(app, bibcode, timeout=30):
    """
    Convert input bibcodes into their canonical form if they exist
    """
    canonical = get_canonical_bibcodes(app, [bibcode], timeout=timeout)
    if len(canonical) == 0:
        return None
    else:
        return canonical[0]
