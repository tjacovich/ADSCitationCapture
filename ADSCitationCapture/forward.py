
import os
import itertools
import datetime
from adsputils import get_date, date2solrstamp
from dateutil.tz import tzutc
from adsmsg import DenormalizedRecord, NonBibRecord, Status, CitationChangeContentType
from bs4 import BeautifulSoup
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
def build_record(app, citation_change, parsed_metadata, citations, db_versions, readers=[], entry_date=None):
    if citation_change.content_type != CitationChangeContentType.doi:
        raise Exception("Only DOI records can be forwarded to master")
    # Extract required values
    bibcode = parsed_metadata.get('bibcode')
    if bibcode is None:
        raise Exception("Only records with a bibcode can be forwarded to master")
    if entry_date is None:
        entry_date = citation_change.timestamp.ToDatetime()
    #Check if doi points to a concept record or to a specific version
    if parsed_metadata.get('version_of', None) not in (None,"",[],''): 
        is_release = True
    else:
        is_release = False
    alternate_bibcode = parsed_metadata.get('alternate_bibcode', [])
    abstract = parsed_metadata.get('abstract', "")
    title = parsed_metadata.get('title', "")
    keywords = parsed_metadata.get('keywords', [])
    authors = parsed_metadata.get('authors', [])
    normalized_authors = parsed_metadata.get('normalized_authors', [])
    affiliations = parsed_metadata.get('affiliations', ['-']*len(authors))
    try:
        pubdate = parsed_metadata.get('pubdate', get_date().strftime("%Y-%m-%d"))
        forward_time = (datetime.datetime.strptime(pubdate, "%Y-%m-%d")+datetime.timedelta(minutes=30)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    except Exception as e:
        logger.error("Extracting publication date for {} failed with Exception {}. Setting to Current Time.".format(bibcode, e))
        pubdate = get_date().strftime("%Y-%m-%d")
        forward_time = (datetime.datetime.strptime(pubdate, "%Y-%m-%d")+datetime.timedelta(minutes=30)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')        

    source = parsed_metadata.get('source', "Unknown")
    version = parsed_metadata.get('version', "")
    doctype = parsed_metadata.get('doctype', "software")
    # Clean abstract and title
    abstract = ''.join(BeautifulSoup(abstract, features="lxml").findAll(text=True)).replace('\n', ' ').replace('\r', '')
    title = ''.join(BeautifulSoup(title, features="lxml").findAll(text=True)).replace('\n', ' ').replace('\r', '')
    # Extract year
    year = pubdate.split("-")[0]
    # Build an author_facet_hier list with the following structure:
    #   "0/Blanco-Cuaresma, S",
    #   "1/Blanco-Cuaresma, S/Blanco-Cuaresma, S",
    #   "0/Soubiran, C",
    #   "1/Soubiran, C/Soubiran, C",
    author_facet_hier = list(itertools.chain.from_iterable(zip(["0/"+a for a in normalized_authors], ["1/"+a[0]+"/"+a[1] for a in zip(normalized_authors, authors)])))

    # Count
    n_keywords = len(keywords)
    n_authors = len(authors)
    n_citations = len(citations)
    doi = citation_change.content
    record_dict = {
        'abstract': abstract,
        'ack': '',
        'aff': [ "-" if aff == "" else aff for aff in affiliations],
        'alternate_bibcode': alternate_bibcode,
        'alternate_title': [],
        'arxiv_class': [],
        'author': authors,
        'author_count': n_authors,
        'author_facet': normalized_authors,
        'author_facet_hier': author_facet_hier,
        'author_norm': normalized_authors,
        'bibcode': bibcode,
        'bibstem': ['zndo'],
        'bibstem_facet': 'zndo',
        'copyright': [],
        'comment': [],
        'database': ['general', 'astronomy'],
        'entry_date': date2solrstamp(entry_date), # date2solrstamp(get_date()),
        'year': year,
        'date': (datetime.datetime.strptime(pubdate, "%Y-%m-%d")+datetime.timedelta(minutes=30)).strftime('%Y-%m-%dT%H:%M:%S.%fZ'), # TODO: Why this date has to be 30 minutes in advance? This is based on ADSImportPipeline SolrAdapter
        'doctype': doctype,
        'doctype_facet_hier': ["0/Non-Article", "1/Non-Article/Software"],
        'doi': [doi],
        'eid': doi,
        'email': ['-']*n_authors,
        'first_author': authors[0] if n_authors > 0 else '',
        'first_author_facet_hier': author_facet_hier[:2],
        'first_author_norm': normalized_authors[0] if n_authors > 0 else '',
        'links_data': ['{{"access": "", "instances": "", "title": "", "type": "electr", "url": "{}"}}'.format(app.conf['DOI_URL'] + doi)], # TODO: How is it different from nonbib?
        'identifier': [bibcode, doi] + alternate_bibcode,
        'esources': ["PUB_HTML"],
        'citation': citations,
        'citation_count': n_citations,
        'citation_count_norm': n_citations/n_authors if n_authors > 0 else 0,
        'data_count': 1, # Number of elements in `links_data`
        'keyword': keywords,
        'keyword_facet': keywords,
        'keyword_norm': ["-"]*n_keywords,
        'keyword_schema': ["-"]*n_keywords,
        'property': ["ESOURCE", "NONARTICLE", "NOT REFEREED", "PUB_OPENACCESS", "OPENACCESS"],
        'pub': source,
        'pub_raw': source,
        'pubdate': pubdate,
        'pubnote': [],
        'read_count': len(readers),
        'title': [title],
        'publisher': source,
        'version': version
    }
    if version is None: # Concept DOIs may not contain version
        del record_dict['version']
    # Status
    if citation_change.status == Status.new:
        status = 2
    elif citation_change.status == Status.updated:
        status = 3
    elif citation_change.status == Status.deleted:
        status = 1
        # Only use this field for deletions, otherwise Solr will complain the field does not exist
        # and if this key does not exist in the dict/protobuf, the message will be
        # treated as new/update by MasterPipeline
        record_dict['status'] = status
    else:
        status = 0 # active
    if db_versions not in [{"":""}, None]:
        record_dict['property'].append('ASSOCIATED')
    if is_release:
        record_dict['property'].append('RELEASE')

    record = DenormalizedRecord(**record_dict)
    nonbib_record = _build_nonbib_record(app, citation_change, record, db_versions, status, readers=readers)
    return record, nonbib_record


def _build_nonbib_record(app, citation_change, record, db_versions, status, readers=[]):
    doi = citation_change.content
    nonbib_record_dict = {
        'status': status,
        'bibcode': record.bibcode,
        'boost': 0.5, # Value between 0 and 1
        'citation_count': record.citation_count,
        'data': [],
        'data_links_rows': [
            {'link_type': 'ESOURCE', 'link_sub_type': 'PUB_HTML',
                     'url': [app.conf['DOI_URL'] + doi], 'title': [''], 'item_count':0},
                     ], # `item_count` only used for DATA and not ESOURCES
        'citation_count_norm': record.citation_count_norm,
        'grants': [],
        'ned_objects': [],
        'norm_cites': 0, # log10-normalized count of citations computed on the classic site but not currently used
        'read_count': record.read_count,
        'readers': readers,
        'simbad_objects': [],
        'total_link_counts': 0 # Only used for DATA and not for ESOURCES
    }
    if db_versions not in [{"":""}, None]:
        nonbib_record_dict['data_links_rows'].append({'link_type': 'ASSOCIATED', 'link_sub_type': '', 
                     'url': db_versions.values(), 'title': db_versions.keys(), 'item_count':0})
    nonbib_record = NonBibRecord(**nonbib_record_dict)
    nonbib_record.esource.extend(record.esources)
    nonbib_record.reference.extend(record.reference)
    nonbib_record.property.extend(record.property)
    return nonbib_record

