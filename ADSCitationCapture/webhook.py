import requests
import json
from adsputils import setup_logging
import adsmsg
import datetime
import os
import errno
import re

logger = setup_logging(__name__)
logger.propagate = False

def _build_data(event_type, original_relationship_name, source_bibcode, target_id, target_id_schema, target_id_url):
    now = datetime.datetime.now()
    data = {
        "RelationshipType": {
            "SubTypeSchema": "DataCite",
            "SubType": original_relationship_name,
            "Name": "References"
        },
        "Source": {
            "Identifier": {
                "IDScheme": "ads",
                "IDURL": "http://adsabs.harvard.edu/abs/{}".format(source_bibcode),
                "ID": source_bibcode
            },
            "Type": {
                "Name": "unknown"
            }
        },
        "LicenseURL": "https://creativecommons.org/publicdomain/zero/1.0/",
        "Target": {
            "Identifier": {
                "IDScheme": target_id_schema,
                "IDURL": target_id_url,
                "ID": target_id
            },
            "Type": {
                "Name": "software"
            }
        },
        "LinkPublicationDate": now.strftime("%Y-%m-%d"),
        "LinkProvider": [
            {
                "Name": "SAO/NASA Astrophysics Data System"
            }
        ]
    }
    return data

def _target_elements(citation_change):
    target_id = citation_change.content
    if citation_change.content_type == adsmsg.CitationChangeContentType.doi:
        target_id_schema = "DOI"
        target_id_url = "https://doi.org"
    elif citation_change.content_type == adsmsg.CitationChangeContentType.pid:
        target_id_schema = "ASCL"
        target_id_url = "http://ascl.net/"
    elif citation_change.content_type == adsmsg.CitationChangeContentType.url:
        target_id_schema = "URL"
        target_id_url = citation_change.content
    else:
        raise Exception("Unknown citation change data type")
    return target_id, target_id_schema, target_id_url

def _source_cites_target(citation_change, deleted=False):
    if deleted:
        event_type = "relation_deleted"
    else:
        event_type = "relation_created"
    original_relationship_name = "Cites"
    target_id, target_id_schema, target_id_url = _target_elements(citation_change)
    source_bibcode = citation_change.citing
    data = _build_data(event_type, original_relationship_name, source_bibcode, target_id, target_id_schema, target_id_url)
    return data

def _source_is_identical_to_target(citation_change, deleted=False):
    if deleted:
        event_type = "relation_deleted"
    else:
        event_type = "relation_created"
    original_relationship_name = "IsIdenticalTo"
    source_bibcode = citation_change.cited
    target_id, target_id_schema, target_id_url = _target_elements(citation_change)
    data = _build_data(event_type, original_relationship_name, source_bibcode, target_id, target_id_schema, target_id_url)
    return data

def _to_data(citation_change):
    if citation_change.status == adsmsg.Status.new:
        return _source_cites_target(citation_change, deleted=False)
    elif citation_change.status == adsmsg.Status.updated and citation_change.cited != '...................' and citation_change.resolved:
        # Only accept cited bibcode if score is 1 (resolved == True), if not the bibcode is just an unresolved attempt
        return _source_is_identical_to_target(citation_change)
    elif citation_change.status == adsmsg.Status.deleted:
        #return _source_cites_target(citation_change, deleted=True)
        ## https://github.com/asclepias/asclepias-broker/issues/24
        logger.error("The broker does not support deletions yet: {}".format(citation_change))
        return {}
    else:
        logger.error("Citation change does not match any defined events: {}".format(citation_change))
        return {}

def emit_event(ads_webhook_url, ads_webhook_auth_token, citation_change, timeout=30):
    event_data = _to_data(citation_change)
    if event_data:
        data = [event_data]
        headers = {}
        headers["Content-Type"] = "application/json"
        headers["Authorization"] = "Bearer {}".format(ads_webhook_auth_token)
        r = requests.post(ads_webhook_url, data=json.dumps(data), headers=headers, timeout=timeout)
        if not r.ok:
            logger.error("Emit event failed with status code '{}': {}".format(r.status_code, r.content))
            raise Exception("HTTP Post to '{}' failed: {}".format(ads_webhook_url, json.dumps(data)))
        else:
            logger.info("Emitted event (citting '%s', content '%s' and timestamp '%s')", citation_change.citing, citation_change.content, citation_change.timestamp.ToJsonString())
        return True
    return False

def mkdir_p(path):
    """
    Creates a directory. Same behaviour as 'mkdir -p'.
    """
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST:
            pass
        else:
            raise

def dump_event(citation_change, prefix="emitted"):
    """
    Save the event in JSON format in the log directory
    """
    event_data = _to_data(citation_change)
    dump_created = False
    if event_data:
        try:
            logs_dirname = os.path.dirname(logger.handlers[0].baseFilename)
        except:
            logger.exception("Logger's target directory not found")
        else:
            timestamp = citation_change.timestamp.ToDatetime().strftime("%Y%m%d_%H%M%S")
            base_dirname = os.path.join(os.path.join(logs_dirname, prefix), timestamp)
            now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            citing = re.sub('[^-\w\s]+', '-', citation_change.citing)
            #cited = re.sub('[^-\w\s]+', '-', citation_change.cited)
            content = re.sub('[^-\w\s]+', '-', citation_change.content)
            filename = "{}_{}_{}.json".format(now, citing, content)
            try:
                if not os.path.exists(base_dirname):
                    mkdir_p(base_dirname)
                json.dump(event_data, open(os.path.join(base_dirname, filename), "w"), indent=2)
            except:
                logger.exception("Impossible to dump event")
            else:
                dump_created = True
    return dump_created
