from dateutil.parser import parse
import requests
import re
import json
from pyingest.parsers.datacite import DataCiteParser
from adsputils import setup_logging

# ============================= INITIALIZATION ==================================== #
logger = setup_logging(__name__)
dc = DataCiteParser()
zenodo_doi_re = re.compile("^10.\d{4,9}/zenodo\.([0-9]*)$", re.IGNORECASE)


# =============================== FUNCTIONS ======================================= #

def fetch_metadata(base_doi_url, doi):
    """
    Fetches DOI metadata in datacite format
    """
    record_found = False
    try_later = False
    doi_endpoint = base_doi_url + doi
    headers = {}
    ## Supported content types: https://citation.crosscite.org/docs.html#sec-4
    #headers["Accept"] = "application/vnd.datacite.datacite+xml;q=1, application/vnd.crossref.unixref+xml;q=1"
    #headers["Accept"] = "application/vnd.crossref.unixref+xml;q=1" # This format does not contain software type tag
    headers["Accept"] = "application/vnd.datacite.datacite+xml;q=1"
    timeout = 30
    try:
        r = requests.get(doi_endpoint, headers=headers, timeout=timeout)
    except:
        logger.exception("HTTP request to DOI service failed: %s", doi_endpoint)
        try_later = True
    else:
        if r.status_code == 406:
            logger.error("No answer from doi.org with the requested format (%s) for: %s", headers["Accept"], doi_endpoint)
        elif r.status_code == 404:
            logger.error("Entry not found (404 HTTP error code): %s", doi_endpoint)
        elif not r.ok:
            # Rest of bad status codes
            try_later = True
            logger.error("HTTP request with error code '%s' for: %s", r.status_code, doi_endpoint)
        else:
            record_found = True

    if try_later:
        # Exceptions make the task to fail, and the framework will re-try automatically later on
        logger.error("HTTP request to DOI service failed: %s", doi_endpoint)
        raise Exception("HTTP request to DOI service failed: {}".format(doi_endpoint))

    return r.content if record_found else None


def build_bibcode(metadata, doi_re, bibstem):
    """
    Builds a bibcode based on the parsed metadata received from datacite. The
    metadata should contain:
        - properties -> DOI [string]
        - pubdate [string "YYYY-MM-DD"]
        - authors [array of strings]

    The DOI string should match the doi_re regular expression, which will
    identify the substring to be used in the final bibcode. The bibstem will be
    also used in the bibcode construction.

    It returns a bibcode [string].
    """
    bibcode = ""
    doi = metadata.get('properties', {}).get('DOI', None)
    if doi is None:
        logger.error("No DOI property provided")
        return bibcode

    doi_match = doi_re.match(doi)
    if doi_match:
        if len(doi_match.groups()) == 1:
            doi_id = doi_match.groups()[0]
        else:
            logger.error("DOI record number not found: '%s'", doi)
            return bibcode
    else:
        logger.error("Unexcepted DOI: '%s'", doi)
        return bibcode

    try:
        year = str(parse(metadata.get('pubdate', "")).year)
    except ValueError, e:
        logger.error("Unknown publication year '%s' for DOI '%s'", metadata.get('pubdate'), doi)
        return bibcode

    if len(metadata.get('authors', [])) >= 1 and len(metadata['authors'][0]) >= 1:
        first_author_last_name_initial = str(metadata['authors'][0][0])
    else:
        logger.error("Unknown first author last name initial '%s' for DOI '%s'", ";".join(metadata.get('authors', [])), doi)
        return bibcode

    bibcode_allowed_size = 19
    bibcode_size = len(year) + len(bibstem) + len(doi_id) + len(first_author_last_name_initial)
    bibcode_size_misalignment = bibcode_size - bibcode_allowed_size
    if bibcode_size_misalignment >= 0:
        doi_id = doi_id[bibcode_size_misalignment:]
    elif bibcode_size_misalignment < 0:
        doi_id = "."*-bibcode_size_misalignment + doi_id
    bibcode = year + bibstem + doi_id + first_author_last_name_initial
    return bibcode

def parse_metadata(raw_metadata):
    """
    It expects metadata in datacite format [string] and returns the parsed
    metadata [dict] unless it does not correspond to a recongised source
    (only zenodo right now) or it is not a software record, in which case a
    None value is returned.
    """
    return _parse_metadata_zenodo_doi(raw_metadata)

def _parse_metadata_zenodo_doi(raw_metadata):
    """
    It expects metadata in datacite format from a zenodo DOI [string] and returns
    the parsed metadata [dict] unless it did not correspond to a software
    record, for which a None value is returned.
    """
    try:
        parsed_metadata = dc.parse(raw_metadata)
    except Exception, e:
        logger.exception("Failed parsing")
        return {}
    parsed_metadata['link_alive'] = True
    is_software = parsed_metadata['doctype'].lower() == "software"

    if is_software:
        zenodo_bibstem = "zndo"
        bibcode = build_bibcode(parsed_metadata, zenodo_doi_re, zenodo_bibstem)
        if bibcode not in (None, ""):
            parsed_metadata['bibcode'] = bibcode
    return parsed_metadata

