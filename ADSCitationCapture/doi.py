import os
from dateutil.parser import parse
import requests
import re
import json
import base64
from pyingest.parsers.datacite import DataCiteParser
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

dc = DataCiteParser()
zenodo_doi_re = re.compile(r"^10.\d{4,9}/zenodo\.([0-9]*)$", re.IGNORECASE)
upper_case_az_character_re = re.compile("[A-Z]")


# =============================== FUNCTIONS ======================================= #
def _fetch_metadata(url, headers={}, timeout=30):
    """
    Fetches DOI metadata
    """
    record_found = False
    try_later = False
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
    except:
        logger.exception("HTTP request failed: %s", url)
        try_later = True
    else:
        if r.status_code == 400:
            logger.error("Bad request due to bad DOI or DOI not activated yet for: %s", url)
        elif r.status_code == 406:
            logger.error("No answer with the requested format (%s) for: %s", headers.get("Accept", "None"), url)
        elif r.status_code == 404:
            logger.error("Entry not found (404 HTTP error code): %s", url)
        elif not r.ok:
            # Rest of bad status codes
            try_later = True
            logger.error("HTTP request with error code '%s' for: %s", r.status_code, url)
        else:
            record_found = True

    content = None
    if not try_later and record_found:
        content = r.text
    return try_later, record_found, content

def _decode_datacite_content(alt_content):
    """
    DataCite API responses are in JSON format and the content that follows
    DataCite XML format is found in ['data']['attributes']['xml'], encoded
    with base64. This function decodes it if it is found, otherwise it returns
    None.
    """
    decoded_alt_content = None
    try:
        alt_json_content = json.loads(alt_content)
    except ValueError:
        pass
    else:
        alt_xml = alt_json_content.get('data', {}).get('attributes', {}).get('xml', None)
        if alt_xml:
            try:
                decoded_alt_content = base64.b64decode(alt_xml).decode('utf8')
            except UnicodeDecodeError:
                pass
    return decoded_alt_content

def fetch_metadata(base_doi_url, base_datacite_url, doi):
    """
    Fetches DOI metadata in datacite format from doi.org or, alternatively,
    api.datacite.org if the former fails
    """
    headers = {}
    ## https://support.datacite.org/docs/datacite-content-resolver
    ## Supported content types: https://citation.crosscite.org/docs.html#sec-4
    #headers["Accept"] = "application/vnd.datacite.datacite+xml;q=1, application/vnd.crossref.unixref+xml;q=1"
    #headers["Accept"] = "application/vnd.crossref.unixref+xml;q=1" # This format does not contain software type tag
    headers["Accept"] = "application/vnd.datacite.datacite+xml;q=1"
    doi_endpoint = base_doi_url + doi
    try_later, record_found, content = _fetch_metadata(doi_endpoint, headers=headers, timeout=30)

    if try_later or not record_found or "<version/>" in content: # TODO: Temporary doi.org/crossref bug where version is not provided
        # Alternative source for metadata
        alt_doi_endpoint = base_datacite_url + doi
        alt_headers = {}
        alt_try_later, alt_record_found, alt_content = _fetch_metadata(alt_doi_endpoint, headers=alt_headers, timeout=30)
        if not alt_try_later and alt_record_found:
            decoded_alt_content = _decode_datacite_content(alt_content)
            if decoded_alt_content:
                try_later = False
                record_found = True
                content = decoded_alt_content

    if try_later:
        # Exceptions make the task to fail, and the framework will re-try automatically later on
        logger.error("HTTP request to DOI service failed: %s", doi_endpoint)
        raise Exception("HTTP request to DOI service failed: {}".format(doi_endpoint))

    return content if record_found else None


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
    except ValueError as e:
        logger.error("Unknown publication year '%s' for DOI '%s'", metadata.get('pubdate'), doi)
        return bibcode

    if len(metadata.get('normalized_authors', [])) >= 1 and len(metadata['normalized_authors'][0]) >= 1:
        first_author_last_name_initial = str(metadata['normalized_authors'][0][0]).upper()
        if upper_case_az_character_re.fullmatch(first_author_last_name_initial) is None:
            # If the first initial of the first author's last name is not a valid A-Z character
            # use '.' instead respecting the bibcode convention
            first_author_last_name_initial = "."
    else:
        logger.error("Unknown first author last name initial '%s' for DOI '%s'", ";".join(metadata.get('normalized_authors', [])), doi)
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
    Author list is also parsed and normalized (keys 'authors' and 'normalized_authors').
    """
    try:
        parsed_metadata = dc.parse(raw_metadata)
    except Exception as e:
        logger.exception("Failed parsing")
        return {}
    parsed_metadata['link_alive'] = True
    is_software = parsed_metadata.get('doctype', '').lower() == "software"

    if is_software:
        zenodo_bibstem = "zndo"
        bibcode = build_bibcode(parsed_metadata, zenodo_doi_re, zenodo_bibstem)
        if bibcode not in (None, ""):
            parsed_metadata['bibcode'] = bibcode
    return parsed_metadata

def fetch_all_versions_doi(parsed_metadata):  
    """
    Takes zenodo parsed metadata and fetches DOI for all versions of zenodo repository
    """
    return _fetch_core_doi(parsed_metadata)

def _fetch_all_versions_doi(parsed_metadata):
    """
    Takes zenodo parsed metadata and fetches DOI for all versions of zenodo repository
    """
    if parsed_metadata.get('version_of',None) not in (None,""):   
        return parsed_metadata.get('version_of',None)
    elif parsed_metadata.get('versions',None) not in (None, ""):
        return parsed_metada.get('properties')['DOI']
    else:
        return None
