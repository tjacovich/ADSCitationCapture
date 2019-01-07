from __future__ import absolute_import, unicode_literals
import requests
import urllib

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


