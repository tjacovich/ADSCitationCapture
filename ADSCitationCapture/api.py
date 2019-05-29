from __future__ import absolute_import, unicode_literals
import requests
import urllib
from adsputils import setup_logging

# ============================= INITIALIZATION ==================================== #
logger = setup_logging(__name__)
logger.propagate = False


# =============================== FUNCTIONS ======================================= #
def _request_citations_page(app, bibcode, start, rows):
    params = urllib.urlencode({
                'fl': 'bibcode',
                'q': 'citations(bibcode:{0})'.format(bibcode),
                'start': start,
                'rows': rows,
                'sort': 'date desc, bibcode desc',
            })
    headers = {}
    headers["Authorization"] = "Bearer:{}".format(app.conf['ADS_API_TOKEN'])
    url = app.conf['ADS_API_URL']+"search/query?"+params
    r = requests.get(url, headers=headers)
    return r.json()

def request_existing_citations(app, bibcode):
    start = 0
    rows = 25
    existing_citation_bibcodes = []
    n_existing_citations = None
    while True:
        answer = _request_citations_page(app, bibcode, start, rows)
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
        params = urllib.urlencode({
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
        data = "bibcode\n" + "\n".join(bibcodes_chunk)
        r_json = {}
        try:
            r = requests.post(url, headers=headers, data=data, timeout=timeout)
        except:
            logger.exception("BigQuery API request failed for bibcodes (chunk: %i/%i): %s", n_chunk, total_n_chunks, " ".join(bibcodes_chunk))
            raise
        else:
            if not r.ok:
                msg = "BigQuery API request with error code '{}' for bibcodes (chunk: {}/{}): {}".format(r.status_code, n_chunk, total_n_chunks, " ".join(bibcodes_chunk))
                logger.error(msg)
                raise Exception(msg)
            else:
                try:
                    r_json = r.json()
                except ValueError:
                    msg = "No JSON object could be decoded from BigQuery API response when searching canonical bibcodes (chunk: {}/{}) for: {}".format(n_chunk, total_n_chunks, " ".join(bibcodes_chunk))
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
