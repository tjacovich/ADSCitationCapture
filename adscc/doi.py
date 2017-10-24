import requests
import json
from lxml import etree
from adsputils import setup_logging

logger = setup_logging(__name__)

def is_software(base_doi_url, doi):
    is_software = False
    record_found = False
    try_later = False
    doi_endpoint = base_doi_url + doi
    headers = {}
    ## Supported content types: https://citation.crosscite.org/docs.html#sec-4
    #headers["Accept"] = "application/vnd.datacite.datacite+xml;q=1, application/vnd.crossref.unixref+xml;q=1"
    #headers["Accept"] = "application/vnd.crossref.unixref+xml;q=1" # This format does not contain software type tag
    headers["Accept"] = "application/vnd.datacite.datacite+xml;q=1"
    data = {}
    timeout = 30
    try:
        r = requests.get(doi_endpoint, data=json.dumps(data), headers=headers, timeout=timeout)
    except:
        logger.exception("HTTP request to DOI service failed: %s", doi_endpoint)
        try_later = True
    else:
        if r.status_code == 406:
            logger.error("No answer from doi.org with the requested format (%s) for: %s", headers["Accept"], doi_endpoint)
            #raise Exception("No answer from doi.org with the requested format ({}) for: {}".format(headers["Accept"], doi_endpoint))
        elif r.status_code == 404:
            logger.error("Entry not found (404 HTTP error code): %s", doi_endpoint)
            #raise Exception("Entry not found (404 HTTP error code): {}".format(doi_endpoint))
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

    if record_found:
        try:
            root = etree.fromstring(r.content)
        except:
            pass
        else:
            resource_type = root.find("{http://datacite.org/schema/kernel-3}resourceType")
            if resource_type is not None:
                resource_type_general = resource_type.get('resourceTypeGeneral')
                is_software = resource_type_general is not None and resource_type_general.lower() == "software"
    return is_software
