import itertools
import datetime
from adsputils import get_date, date2solrstamp
from dateutil.tz import tzutc
from adsmsg import DenormalizedRecord, NonBibRecord, Status, CitationChangeContentType
from bs4 import BeautifulSoup
from adsputils import setup_logging

# ============================= INITIALIZATION ==================================== #
logger = setup_logging(__name__)


# =============================== FUNCTIONS ======================================= #
def build_record(app, citation_change, parsed_metadata, citations):
    if citation_change.content_type != CitationChangeContentType.doi:
        raise Exception("Only DOI records can be forwarded to master")
    # Extract required values
    bibcode = parsed_metadata.get('bibcode')
    if bibcode is None:
        raise Exception("Only records with a bibcode can be forwarded to master")
    abstract = parsed_metadata.get('abstract', u"")
    title = parsed_metadata.get('title', u"")
    keywords = parsed_metadata.get('keywords', [])
    authors = parsed_metadata.get('authors', [])
    normalized_authors = parsed_metadata.get('normalized_authors', [])
    affiliations = parsed_metadata.get('affiliations', [u'-']*len(authors))
    pubdate = parsed_metadata.get('pubdate', get_date().strftime("%Y-%m-%d"))
    source = parsed_metadata.get('source', u"Unknown")
    version = parsed_metadata.get('version', u"")
    doctype = parsed_metadata.get('doctype', u"software")
    # Clean abstract and title
    abstract = u''.join(BeautifulSoup(abstract, features="lxml").findAll(text=True)).replace('\n', ' ').replace('\r', '')
    title = u''.join(BeautifulSoup(title, features="lxml").findAll(text=True)).replace('\n', ' ').replace('\r', '')
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
        'ack': u'',
        'aff': [ u"-" if aff == "" else aff for aff in affiliations],
        'alternate_bibcode': [],
        'alternate_title': [],
        'arxiv_class': [],
        'author': authors,
        'author_count': n_authors,
        'author_facet': normalized_authors,
        'author_facet_hier': author_facet_hier,
        'author_norm': normalized_authors,
        'bibcode': bibcode,
        'bibstem': [u'zndo'],
        'bibstem_facet': u'zndo',
        'copyright': [],
        'comment': [],
        'database': [u'general', u'astronomy'],
        'entry_date': date2solrstamp(citation_change.timestamp.ToDatetime()), # date2solrstamp(get_date()),
        'year': year,
        'date': (citation_change.timestamp.ToDatetime()+datetime.timedelta(minutes=30)).strftime('%Y-%m-%dT%H:%M:%S.%fZ'), # TODO: Why this date has to be 30 minutes in advance? This is based on ADSImportPipeline SolrAdapter
        'doctype': doctype,
        'doctype_facet_hier': [u"0/Non-Article", u"1/Non-Article/Software"],
        'doi': [doi],
        'eid': doi,
        'email': [u'-']*n_authors,
        'first_author': authors[0] if n_authors > 0 else u'',
        'first_author_facet_hier': author_facet_hier[:2],
        'first_author_norm': normalized_authors[0] if n_authors > 0 else u'',
        'links_data': [u'{{"access": "", "instances": "", "title": "", "type": "electr", "url": "{}"}}'.format(app.conf['DOI_URL'] + doi)], # TODO: How is it different from nonbib?
        'identifier': [bibcode, doi],
        'esources': [u"PUB_HTML"],
        'citation': citations,
        'citation_count': n_citations,
        'citation_count_norm': n_citations/n_authors if n_authors > 0 else 0,
        'data_count': 1, # Number of elements in `links_data`
        'keyword': keywords,
        'keyword_facet': keywords,
        'keyword_norm': [u"-"]*n_keywords,
        'keyword_schema': [u"-"]*n_keywords,
        'property': [u"ESOURCE", u"NONARTICLE", u"NOT REFEREED", u"PUB_OPENACCESS", u"OPENACCESS"],
        'pub': source,
        'pub_raw': source,
        'pubdate': pubdate,
        'pubnote': [],
        'read_count': 0,
        'title': [title],
        'publisher': source,
        'version': version
    }
    # Status
    if citation_change.status == Status.new:
        status = 2
    elif citation_change.status == Status.updated:
        status = 3
    elif citation_change.status == Status.deleted:
        status = 1
        # Only use this for deletions, otherwise Solr will complain the field does not exist
        # and if this key does not exist in the dict/protobuf, the message will be
        # treated as new/update by MasterPipeline
        record_dict['status'] = status
    else:
        status = 0 # active
    record = DenormalizedRecord(**record_dict)
    nonbib_record = _build_nonbib_record(app, citation_change, record, status)
    return record, nonbib_record


def _build_nonbib_record(app, citation_change, record, status):
    doi = citation_change.content
    nonbib_record_dict = {
        'status': status,
        'bibcode': record.bibcode,
        'boost': 0.5, # Value between 0 and 1
        'citation_count': record.citation_count,
        'data': [],
        'data_links_rows': [
            {'link_type': 'ESOURCE', 'link_sub_type': 'PUB_HTML',
                     'url': [app.conf['DOI_URL'] + doi], 'title': [''], 'item_count':0}, # `item_count` only used for DATA and not ESOURCES
        ],
        'citation_count_norm': record.citation_count_norm,
        'grants': [],
        'ned_objects': [],
        'norm_cites': 0, # log10-normalized count of citations computed on the classic site but not currently used
        'read_count': record.read_count,
        'readers': [],
        'simbad_objects': [],
        'total_link_counts': 0 # Only used for DATA and not for ESOURCES
    }
    nonbib_record = NonBibRecord(**nonbib_record_dict)
    nonbib_record.esource.extend(record.esources)
    nonbib_record.reference.extend(record.reference)
    nonbib_record.property.extend(record.property)
    return nonbib_record

