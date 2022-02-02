import os
from kombu import Queue
from google.protobuf.json_format import MessageToDict
from datetime import datetime
import ADSCitationCapture.app as app_module
import ADSCitationCapture.webhook as webhook
import ADSCitationCapture.doi as doi
import ADSCitationCapture.url as url
import ADSCitationCapture.db as db
import ADSCitationCapture.forward as forward
import ADSCitationCapture.api as api
import adsmsg

# ============================= INITIALIZATION ==================================== #

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
app = app_module.ADSCitationCaptureCelery('ads-citation-capture', proj_home=proj_home, local_config=globals().get('local_config', {}))
logger = app.logger

app.conf.CELERY_QUEUES = (
    Queue('process-citation-changes', app.exchange, routing_key='process-citation-changes'),
    Queue('process-github-urls', app.exchange, routing_key='process-github-urls'),
    Queue('process-new-citation', app.exchange, routing_key='process-new-citation'),
    Queue('process-updated-citation', app.exchange, routing_key='process-updated-citation'),
    Queue('process-deleted-citation', app.exchange, routing_key='process-deleted-citation'),
    Queue('process-updated-association-works', app.exchange, routing_key='process-updated-associated-works'),
    Queue('maintenance_canonical', app.exchange, routing_key='maintenance_canonical'),
    Queue('maintenance_metadata', app.exchange, routing_key='maintenance_meadata'),
    Queue('maintenance_resend', app.exchange, routing_key='maintenance_resend'),
    Queue('maintenance_reevaluate', app.exchange, routing_key='maintenance_reevaluate'),
    Queue('maintenance_associated_works', app.exchange, routing_key='maintenance_associated_works'),
    Queue('output-results', app.exchange, routing_key='output-results'),
)

#limit github API queries to keep below rate limit
github_api_limit = app.conf.get('GITHUB_API_LIMIT', '80/m')

# ============================= TASKS ============================================= #

@app.task(queue='process-new-citation')
def task_process_new_citation(citation_change, force=False):
    """
    Process new citation:
    - Retrieve metadata from doi.org
    """
    canonical_citing_bibcode = api.get_canonical_bibcode(app, citation_change.citing)
    if canonical_citing_bibcode is None:
        logger.error("The citing bibcode '%s' is not in the system yet, it will be skipped in this ingestion", citation_change.citing)
        return
    content_type = None
    is_link_alive = False
    status = "DISCARDED"

    # Check if we already have the citation target in the DB
    metadata = db.get_citation_target_metadata(app, citation_change.content)
    citation_target_in_db = bool(metadata) # False if dict is empty
    raw_metadata = metadata.get('raw', None)
    parsed_metadata = metadata.get('parsed', {})
    if citation_target_in_db:
        status = metadata.get('status', 'DISCARDED') # "REGISTERED" if it is a software record

    #Zenodo
    if citation_change.content_type == adsmsg.CitationChangeContentType.doi \
        and citation_change.content not in ["", None]:
        # Default values
        content_type = "DOI"
        associated_version_bibcodes = {'':''}
        if not citation_target_in_db:
            # Fetch DOI metadata (if HTTP request fails, an exception is raised
            # and the task will be re-queued (see app.py and adsputils))
            raw_metadata = doi.fetch_metadata(app.conf['DOI_URL'], app.conf['DATACITE_URL'], citation_change.content)
            if raw_metadata:
                parsed_metadata = doi.parse_metadata(raw_metadata)
                is_software = parsed_metadata.get('doctype', '').lower() == "software"
                if parsed_metadata.get('bibcode') not in (None, "") and is_software:
                    status = "REGISTERED"
                    #Check for additional versions
                    try:
                        all_versions_doi = doi.fetch_all_versions_doi(app.conf['DOI_URL'], app.conf['DATACITE_URL'], parsed_metadata)
                    except:
                        logger.error("Unable to recover related versions for {}",citation_change)
                        all_versions_doi = None
                    #fetch additional versions from db if they exist.
                    if all_versions_doi['versions'] not in (None,[]):
                        logger.info("Found {} versions for {}".format(len(all_versions_doi['versions']),citation_change.content))
                        versions_in_db=db.get_associated_works_by_doi(app, all_versions_doi)
                        #Only add bibcodes if there are versions in db, otherwise leave as None.
                        if versions_in_db not in (None, [None]):
                            logger.info("Found {} versions in database for {}".format(len(versions_in_db),citation_change.content))
                            #adds the new citation target bibcode because it will not be in the db yet, 
                            # and then appends the versions already in the db.
                            associated_version_bibcodes = {parsed_metadata.get('version'):parsed_metadata.get('bibcode')}
                            associated_version_bibcodes.update(versions_in_db)
                            logger.debug("{}: associated_versions_bibcodes".format(associated_version_bibcodes))
                            for bibcode in versions_in_db.values():
                                associated_registered_record = db.get_citation_targets_by_bibcode(app,[bibcode])[0] 
                                associated_citation_change = adsmsg.CitationChange(content=associated_registered_record['content'],
                                                       content_type=getattr(adsmsg.CitationChangeContentType, associated_registered_record['content_type'].lower()),
                                                       status=adsmsg.Status.updated,
                                                       timestamp=datetime.now()
                                                       )
                                #update associated works for all versions in db
                                logger.info('Calling task process_updated_associated_works')
                                task_process_updated_associated_works.delay(associated_citation_change, associated_version_bibcodes)
                        
    #PID
    elif citation_change.content_type == adsmsg.CitationChangeContentType.pid \
        and citation_change.content not in ["", None]:
        content_type = "PID"
        status = None
        is_link_alive = url.is_alive(app.conf['ASCL_URL'] + citation_change.content)
        parsed_metadata = {'link_alive': is_link_alive, "doctype": "software" }

    #URL
    elif citation_change.content_type == adsmsg.CitationChangeContentType.url \
        and citation_change.content not in ["", None]:
        content_type = "URL"
        is_link_alive = url.is_alive(citation_change.content)
        status = "EMITTABLE"
        license_info = {'license_name': "", 'license_url': ""}
        #If link is alive, attempt to get license info from github. Else return empty license.
        if url.is_github(citation_change.content):
            task_process_github_urls.delay(citation_change, metadata)
        else:
            status = "DISCARDED"
        parsed_metadata = {'link_alive': is_link_alive, 'doctype': 'unknown', 'license_name': license_info.get('license_name', ""), 'license_url': license_info.get('license_url', "") }

    else:
        logger.error("Citation change should have doi, pid or url informed: {}", citation_change)
        status = None

    #Generates entry for Zenodo citations and notifies web broker
    if status not in [None, "EMITTABLE"]:
        if not citation_target_in_db:
            # Create citation target in the DB
            target_stored = db.store_citation_target(app, citation_change, content_type, raw_metadata, parsed_metadata, status, associated_version_bibcodes)
        
        if status == "REGISTERED":
            #Connects new bibcode to canonical bibcode and DOI
            if citation_change.content_type == adsmsg.CitationChangeContentType.doi:

                if canonical_citing_bibcode != citation_change.citing:
                    # These two bibcodes are identical (point to same source) and we can signal the broker
                    event_data = webhook.identical_bibcodes_event_data(citation_change.citing, canonical_citing_bibcode)
                    if event_data:
                        dump_prefix = citation_change.timestamp.ToDatetime().strftime("%Y%m%d_%H%M%S")
                        logger.debug("Calling 'task_emit_event' for '%s' IsIdenticalTo '%s'", citation_change.citing, canonical_citing_bibcode)
                        task_emit_event.delay(event_data, dump_prefix)

                citation_target_bibcode = parsed_metadata.get('bibcode')

                # The new bibcode and the DOI are identical
                event_data = webhook.identical_bibcode_and_doi_event_data(citation_target_bibcode, citation_change.content)
                if event_data:
                    dump_prefix = citation_change.timestamp.ToDatetime().strftime("%Y%m%d_%H%M%S")
                    logger.debug("Calling 'task_emit_event' for '%s' IsIdenticalTo '%s'", citation_target_bibcode, citation_change.content)
                    task_emit_event.delay(event_data, dump_prefix)

                # Get citations from the database and transform the stored bibcodes into their canonical ones as registered in Solr.
                original_citations = db.get_citations_by_bibcode(app, citation_target_bibcode)
                citations = api.get_canonical_bibcodes(app, original_citations)

                # Add canonical bibcode of current detected citation
                if canonical_citing_bibcode and canonical_citing_bibcode not in citations:
                    citations.append(canonical_citing_bibcode)

                logger.debug("Calling 'task_output_results' with '%s'", citation_change)
                task_output_results.delay(citation_change, parsed_metadata, citations, associated_version_bibcodes)
            logger.debug("Calling '_emit_citation_change' with '%s'", citation_change)

            _emit_citation_change(citation_change, parsed_metadata)
        # Store the citation at the very end, so that if an exception is raised before
        # this task can be re-run in the future without key collisions in the database
        stored = db.store_citation(app, citation_change, content_type, raw_metadata, parsed_metadata, status)
    
@app.task(queue='process-github-urls', rate_limit = github_api_limit)
def task_process_github_urls(citation_change, metadata):
    """
    Process new github urls
    Emit to broker only if it is EMITTABLE
    Do not forward to Master
    """
    logger.info("Processing citation to github url: {}".format(citation_change.content))
    github_api_mode = app.conf.get('GITHUB_API_MODE', False)
    citation_target_in_db = bool(metadata) # False if dict is empty
    raw_metadata = metadata.get('raw', None)
    parsed_metadata = metadata.get('parsed', {})
    content_type = "URL"
    is_link_alive = url.is_alive(citation_change.content)
    status = "EMITTABLE"
    license_info = {'license_name': "", 'license_url': ""}
    #If link is alive, attempt to get license info from github. Else return empty license.
    if url.is_github(citation_change.content) and is_link_alive:
        if github_api_mode:
            license_info = api.get_github_metadata(app, citation_change.content)
    elif not url.is_github(citation_change.content):
        status = "DISCARDED"
        logger.debug("Citation to github url {} discarded".format(citation_change.content))
    parsed_metadata = {'link_alive': is_link_alive, 'doctype': "unknown", 'license_name': license_info.get('license_name', ""), 'license_url': license_info.get('license_url', "") }
    
    #Confirm citation hasn't been added to database as TOF between calling task and when task can actually be executed is potentially quite long.
    metadata = db.get_citation_target_metadata(app, citation_change.content)
    citation_target_in_db = bool(metadata) # False if dict is empty

    #Saves citations to database, and emits citations with "EMITTABLE"
    if status is not None:
        if not citation_target_in_db:
            # Create citation target in the DB
            target_stored = db.store_citation_target(app, citation_change, content_type, raw_metadata, parsed_metadata, status)
        if status=="EMITTABLE":
            logger.debug("Reached 'call _emit_citation_change' with '%s'", citation_change)
            #Emits citation change to broker.
            _emit_citation_change(citation_change, parsed_metadata)
        # Store the citation at the very end, so that if an exception is raised before
        # this task can be re-run in the future without key collisions in the database
        stored = db.store_citation(app, citation_change, content_type, raw_metadata, parsed_metadata, status)

@app.task(queue='process-updated-citation')
def task_process_updated_citation(citation_change, force=False):
    """
    Update citation record
    Emit/forward the update only if it is REGISTERED
    """
    updated = db.update_citation(app, citation_change)
    metadata = db.get_citation_target_metadata(app, citation_change.content)
    parsed_metadata = metadata.get('parsed', {})
    citation_target_bibcode = parsed_metadata.get('bibcode', None)
    status = metadata.get('status', 'DISCARDED')
    # Emit/forward the update only if status is "REGISTERED"
    if updated and status == 'REGISTERED':
        if citation_change.content_type == adsmsg.CitationChangeContentType.doi:
            # Get citations from the database and transform the stored bibcodes into their canonical ones as registered in Solr.
            original_citations = db.get_citations_by_bibcode(app, citation_target_bibcode)
            citations = api.get_canonical_bibcodes(app, original_citations)
            logger.debug("Calling 'task_output_results' with '%s'", citation_change)
            task_output_results.delay(citation_change, parsed_metadata, citations)
        logger.debug("Calling '_emit_citation_change' with '%s'", citation_change)
        _emit_citation_change(citation_change, parsed_metadata)
        
@app.task(queue='process-updated-associated-works')
def task_process_updated_associated_works(citation_change, associated_versions, force=False):
    """
    Update associated works in citation record
    Do not emit to broker as changes to associated works are not propagated
    """
    #check if associated works is not empty
    updated = bool(associated_versions)
    metadata = db.get_citation_target_metadata(app, citation_change.content)
    raw_metadata = doi.fetch_metadata(app.conf['DOI_URL'], app.conf['DATACITE_URL'], citation_change.content)
    if raw_metadata:
        parsed_metadata = doi.parse_metadata(raw_metadata)
        is_software = parsed_metadata.get('doctype', '').lower() == "software"
    #parsed_metadata = metadata.get('parsed', {})
    citation_target_bibcode = parsed_metadata.get('bibcode', None)
    status = metadata.get('status', 'DISCARDED')
    #Forward the update only if status is "REGISTERED" and associated works is not None.
    if status == 'REGISTERED' and updated:
        if citation_change.content_type == adsmsg.CitationChangeContentType.doi:
            # Get citations from the database and transform the stored bibcodes into their canonical ones as registered in Solr.
            original_citations = db.get_citations_by_bibcode(app, citation_target_bibcode)
            citations = api.get_canonical_bibcodes(app, original_citations)
            logger.debug("Calling 'task_output_results' with '%s'", citation_change)
            task_output_results.delay(citation_change, parsed_metadata, citations, associated_versions)
            db.update_citation_target_metadata(app, citation_change.content, raw_metadata, parsed_metadata, associated = associated_versions)
       
@app.task(queue='process-deleted-citation')
def task_process_deleted_citation(citation_change, force=False):
    """
    Mark a citation as deleted
    """
    marked_as_deleted, previous_status = db.mark_citation_as_deleted(app, citation_change)
    metadata = db.get_citation_target_metadata(app, citation_change.content)
    parsed_metadata = metadata.get('parsed', {})
    citation_target_bibcode = parsed_metadata.get('bibcode', None)
    # Emit/forward the update only if the previous status was "REGISTERED"
    if marked_as_deleted and previous_status == 'REGISTERED':
        if citation_change.content_type == adsmsg.CitationChangeContentType.doi:
            # Get citations from the database and transform the stored bibcodes into their canonical ones as registered in Solr.
            original_citations = db.get_citations_by_bibcode(app, citation_target_bibcode)
            citations = api.get_canonical_bibcodes(app, original_citations)
            logger.debug("Calling 'task_output_results' with '%s'", citation_change)
            task_output_results.delay(citation_change, parsed_metadata, citations)
        logger.debug("Calling '_emit_citation_change' with '%s'", citation_change)
        _emit_citation_change(citation_change, parsed_metadata)

def _protobuf_to_adsmsg_citation_change(pure_protobuf):
    """
    Transforms pure citation_change protobuf to adsmsg.CitationChange,
    which can be safely sent via Celery/RabbitMQ.
    """
    tmp = MessageToDict(pure_protobuf, preserving_proto_field_name=True)
    if 'content_type' in tmp:
        # Convert content_type from string to value
        tmp['content_type'] = getattr(adsmsg.CitationChangeContentType, tmp['content_type'])
    else:
        tmp['content_type'] = 0 # default: adsmsg.CitationChangeContentType.doi
    recover_timestamp = False
    if 'timestamp' in tmp:
        # Ignore timestamp in string format
        # 'timestamp': '2019-01-03T21:00:02.010610Z'
        del tmp['timestamp']
        recover_timestamp = True
    citation_change =  adsmsg.CitationChange(**tmp)
    if recover_timestamp:
        # Recover timestamp in google.protobuf.timestamp_pb2.Timestamp format
        # 'timestamp': seconds: 1546549202 nanos: 10610000
        citation_change.timestamp = pure_protobuf.timestamp
    return citation_change

@app.task(queue='process-citation-changes')
def task_process_citation_changes(citation_changes, force=False):
    """
    Process citation changes
    """
    logger.debug('Checking content: %s', citation_changes)
    for citation_change in citation_changes.changes:
        citation_change = _protobuf_to_adsmsg_citation_change(citation_change)
        # Check: Is this citation already stored in the DB?
        citation_in_db = db.citation_already_exists(app, citation_change)

        if citation_change.status == adsmsg.Status.new:
            if citation_in_db:
                logger.error("Ignoring new citation (citing '%s', content '%s' and timestamp '%s') because it already exists in the database", citation_change.citing, citation_change.content, citation_change.timestamp.ToJsonString())
            else:
                logger.debug("Calling 'task_process_new_citation' with '%s'", citation_change)
                task_process_new_citation.delay(citation_change, force=force)
        elif citation_change.status == adsmsg.Status.updated:
            if not citation_in_db:
                logger.error("Ignoring updated citation (citing '%s', content '%s' and timestamp '%s') because it does not exist in the database", citation_change.citing, citation_change.content, citation_change.timestamp.ToJsonString())
            else:
                logger.debug("Calling 'task_process_updated_citation' with '%s'", citation_change)
                task_process_updated_citation.delay(citation_change, force=force)
        elif citation_change.status == adsmsg.Status.deleted:
            if not citation_in_db:
                logger.error("Ignoring deleted citation (citing '%s', content '%s' and timestamp '%s') because it does not exist in the database", citation_change.citing, citation_change.content, citation_change.timestamp.ToJsonString())
            else:
                logger.debug("Calling 'task_process_deleted_citation' with '%s'", citation_change)
                task_process_deleted_citation.delay(citation_change)

def _emit_citation_change(citation_change, parsed_metadata):
    """
    Emit citation change event if the target is a software record
    """
    is_link_alive = parsed_metadata and parsed_metadata.get("link_alive", False)
    is_software = parsed_metadata and parsed_metadata.get("doctype", "").lower() == "software"
    is_emittable = parsed_metadata and citation_change.content_type == adsmsg.CitationChangeContentType.url

    if is_software and is_link_alive:
        event_data = webhook.citation_change_to_event_data(citation_change, parsed_metadata)
        if event_data:
            dump_prefix = citation_change.timestamp.ToDatetime().strftime("%Y%m%d_%H%M%S")
            logger.debug("Calling 'task_emit_event' for '%s'", citation_change)
            task_emit_event.delay(event_data, dump_prefix)

    elif is_emittable and is_link_alive:
        event_data = webhook.citation_change_to_event_data(citation_change, parsed_metadata)
        if event_data:
            dump_prefix = citation_change.timestamp.ToDatetime().strftime("%Y%m%d_%H%M%S")
            logger.debug("Calling 'task_emit_event' for EMITTABLE citation '%s'", citation_change)
            task_emit_event.delay(event_data, dump_prefix)

@app.task(queue='process-emit-event')
def task_emit_event(event_data, dump_prefix):
    """
    Emit event
    """
    emitted = False
    relationship = event_data.get("RelationshipType", {}).get("SubType", None)
    source_id = event_data.get("Source", {}).get("Identifier", {}).get("ID", None)
    target_id = event_data.get("Target", {}).get("Identifier", {}).get("ID", None)

    if not app.conf['TESTING_MODE']:
        prefix = os.path.join("emitted", relationship)
        emitted = webhook.emit_event(app.conf['ADS_WEBHOOK_URL'], app.conf['ADS_WEBHOOK_AUTH_TOKEN'], event_data)
    else:
        prefix = os.path.join("emulated", relationship)
        emitted = True
    if isinstance(dump_prefix, str):
        prefix = os.path.join(prefix, dump_prefix)
    webhook.dump_event(event_data, prefix=prefix)
    stored = db.store_event(app, event_data)

    if app.conf['TESTING_MODE'] and emitted:
        logger.debug("Emulated emission of event due to 'testing mode' (relationship '%s', source '%s' and target '%s')", relationship, source_id, target_id)
    elif emitted:
        logger.debug("Emitted event (relationship '%s', source '%s' and target '%s')", relationship, source_id, target_id)
    else:
        logger.debug("Non-emitted event (relationship '%s', source '%s' and target '%s')", relationship, source_id, target_id)

def _remove_duplicated_dict_in_list(l):
    return [x for x in l if x['content'] in set([r['content'] for r in l])]

@app.task(queue='maintenance_canonical')
def task_maintenance_canonical(dois, bibcodes):
    """
    Maintenance operation:
    - Get all the registered citation targets (or only a subset of them if DOIs and/or bibcodes are specified)
    - For each, get their citations bibcodes and transform them to their canonical form
    - Send to master an update with the new list of citations canonical bibcodes
    """

    n_requested = len(dois) + len(bibcodes)
    if n_requested == 0:
        registered_records = db.get_citation_targets(app, only_status='REGISTERED')
    else:
        registered_records = db.get_citation_targets_by_bibcode(app, bibcodes, only_status='REGISTERED')
        registered_records += db.get_citation_targets_by_doi(app, dois, only_status='REGISTERED')
        registered_records = _remove_duplicated_dict_in_list(registered_records)

    for registered_record in registered_records:
        try:
            # Get citations from the database and transform the stored bibcodes into their canonical ones as registered in Solr.
            original_citations = db.get_citations_by_bibcode(app, registered_record['bibcode'])
            existing_citation_bibcodes = api.get_canonical_bibcodes(app, original_citations)
        except:
            logger.exception("Failed API request to retreive existing citations for bibcode '{}'".format(registered_record['bibcode']))
            continue
        custom_citation_change = adsmsg.CitationChange(content=registered_record['content'],
                                                       content_type=getattr(adsmsg.CitationChangeContentType, registered_record['content_type'].lower()),
                                                       status=adsmsg.Status.updated,
                                                       timestamp=datetime.now()
                                                       )
        parsed_metadata = db.get_citation_target_metadata(app, custom_citation_change.content).get('parsed', {})
        if parsed_metadata:
            logger.debug("Calling 'task_output_results' with '%s'", custom_citation_change)
            task_output_results.delay(custom_citation_change, parsed_metadata, existing_citation_bibcodes)

@app.task(queue='maintenance_metadata')
def task_maintenance_metadata(dois, bibcodes, reset = False):
    """
    Maintenance operation:
    - Get all the registered citation targets (or only a subset of them if DOIs and/or bibcodes are specified)
    - For each, retreive metadata and if it is different to what we have in our database:
        - Get the citations bibcodes and transform them to their canonical form
        - Send to master an update with the new metadata and the current list of citations canonical bibcodes
    """
    n_requested = len(dois) + len(bibcodes)
    if n_requested == 0:
        registered_records = db.get_citation_targets(app, only_status='REGISTERED')
    else:
        registered_records = db.get_citation_targets_by_bibcode(app, bibcodes, only_status='REGISTERED')
        registered_records += db.get_citation_targets_by_doi(app, dois, only_status='REGISTERED')
        registered_records = _remove_duplicated_dict_in_list(registered_records)

    for registered_record in registered_records:
        updated = False
        bibcode_replaced = {}
        # Fetch DOI metadata (if HTTP request fails, an exception is raised
        # and the task will be re-queued (see app.py and adsputils))

        curated_metadata = registered_record.get('curated_metadata', None)

        logger.debug("Curated metadata for {} is {}".format(registered_record['content'], registered_record['curated_metadata']))    
        raw_metadata = doi.fetch_metadata(app.conf['DOI_URL'], app.conf['DATACITE_URL'], registered_record['content'])
        if raw_metadata:
            parsed_metadata = doi.parse_metadata(raw_metadata)
            is_software = parsed_metadata.get('doctype', '').lower() == "software"
            bibcode = registered_record.get('bibcode', None)
            if not is_software:
                logger.error("The new metadata for '%s' has changed its 'doctype' and it is not 'software' anymore", registered_record['bibcode'])
            elif parsed_metadata.get('bibcode') in (None, ""):
                logger.error("The new metadata for '%s' affected the metadata parser and it did not correctly compute a bibcode", registered_record['bibcode'])
            else:
                # Detect concept DOIs: they have one or more versions of the software
                # and they are not a version of something else
                concept_doi = len(parsed_metadata.get('version_of', [])) == 0 and len(parsed_metadata.get('versions', [])) >= 1
                different_bibcodes = registered_record['bibcode'] != parsed_metadata['bibcode']
                if concept_doi and different_bibcodes:
                    # Concept DOI publication date changes with newer software version
                    # and authors can also change (i.e., first author last name initial)
                    # but we want to respect the year in the bibcode, which corresponds
                    # to the year of the latest release when it was first ingested
                    # by ADS
                    parsed_metadata['bibcode'] = registered_record['bibcode']
                    # Temporary bugfix (some bibcodes have non-capital letter at the end):
                    parsed_metadata['bibcode'] = parsed_metadata['bibcode'][:-1] + parsed_metadata['bibcode'][-1].upper()
                    # Re-verify if bibcodes are still different (they could be if
                    # name parsing has changed):
                    different_bibcodes = registered_record['bibcode'] != parsed_metadata['bibcode']
                if different_bibcodes:
                    # These two bibcodes are identical and we can signal the broker
                    event_data = webhook.identical_bibcodes_event_data(registered_record['bibcode'], parsed_metadata['bibcode'])
                    if event_data:
                        dump_prefix = datetime.now().strftime("%Y%m%d") # "%Y%m%d_%H%M%S"
                        logger.debug("Calling 'task_emit_event' for '%s' IsIdenticalTo '%s'", registered_record['bibcode'], parsed_metadata['bibcode'])
                        task_emit_event.delay(event_data, dump_prefix)
                    #
                    logger.warn("Parsing the new metadata for citation target '%s' produced a different bibcode: '%s'. The former will be moved to the 'alternate_bibcode' list, and the new one will be used as the main one.", registered_record['bibcode'], parsed_metadata.get('bibcode', None))
                    alternate_bibcode = parsed_metadata.get('alternate_bibcode', [])
                    alternate_bibcode += registered_record.get('alternate_bibcode', [])
                    if registered_record['bibcode'] not in alternate_bibcode:
                        alternate_bibcode.append(registered_record['bibcode'])
                    parsed_metadata['alternate_bibcode'] = alternate_bibcode
                    bibcode = parsed_metadata.get('bibcode', None)
                    bibcode_replaced = {'previous': registered_record['bibcode'], 'new': parsed_metadata['bibcode'] }
                
                #Protect curated metadata from being bulldozed by metadata updates. 
                if curated_metadata is not {}:
                    modified_metadata = db.generate_modified_metadata(parsed_metadata, curated_metadata)
                    zenodo_bibstem = "zndo"
                    new_bibcode = doi.build_bibcode(modified_metadata, doi.zenodo_doi_re, zenodo_bibstem)
                    alternate_bibcode = registered_record.get('alternate_bibcode', [])
                    parsed_metadata['alternate_bibcode'] = registered_record.get('alternate_bibcode', [])
                    if 'alternate_bibcode' in curated_metadata.keys():
                        alternate_bibcode = list(set(alternate_bibcode+curated_metadata['alternate_bibcode']))
                        logger.debug('alternate bibcodes are {}'.format(alternate_bibcode))
                    bibcode = registered_record.get('bibcode')
                    if new_bibcode != registered_record.get('bibcode'):
                        bibcode = new_bibcode
                        logger.warn("Parsing the new metadata for citation target '%s' produced a different bibcode: '%s'. The former will be moved to the 'alternate_bibcode' list, and the new one will be used as the main one.", registered_record['bibcode'],new_bibcode)
                        if registered_record.get('bibcode') not in alternate_bibcode:
                            #generate complete alt bibcode list including any curated entries
                            alternate_bibcode.append(registered_record.get('bibcode'))
                            #Add the CC generated bibcode to the parsed metadata
                            parsed_metadata['alternate_bibcode'].append(registered_record.get('bibcode'))
                    parsed_metadata['alternate_bibcode'] = list(set(parsed_metadata.get('alternate_bibcode')))
                    modified_metadata['bibcode'] = new_bibcode
                    bibcode_replaced = {'previous': registered_record['bibcode'], 'new': parsed_metadata['bibcode'] }
                    alt_bibcodes = list(set(alternate_bibcode))
                    modified_metadata['alternate_bibcode'] = alt_bibcodes
                    curated_metadata['alternate_bibcode'] = alt_bibcodes
                else:
                    modified_metadata = parsed_metadata
                
                updated = db.update_citation_target_metadata(app, registered_record['content'], raw_metadata, parsed_metadata, curated_metadata = curated_metadata, bibcode = bibcode)
        
        if updated:
            citation_change = adsmsg.CitationChange(content=registered_record['content'],
                                                           content_type=getattr(adsmsg.CitationChangeContentType, registered_record['content_type'].lower()),
                                                           status=adsmsg.Status.updated,
                                                           timestamp=datetime.now()
                                                           )
            if citation_change.content_type == adsmsg.CitationChangeContentType.doi:
                # Get citations from the database and transform the stored bibcodes into their canonical ones as registered in Solr.
                original_citations = db.get_citations_by_bibcode(app, registered_record['bibcode'])
                citations = api.get_canonical_bibcodes(app, original_citations)
                logger.debug("Calling 'task_output_results' with '%s'", citation_change)
                task_output_results.delay(citation_change, modified_metadata, citations, bibcode_replaced=bibcode_replaced)     

@app.task(queue='maintenance_metadata')
def task_maintenance_curation(dois, bibcodes, curated_entries, reset = False):
    """
    Maintenance operation:
    - Get all the registered citation targets for the entries specified in curated_entries
    - For each, retreive metadata and if it is different to what we have in our database:
        - Get the citations bibcodes and transform them to their canonical form
        - Replace the retrieved metadata for values specified in curated_entries
        - Send to master an update with the new metadata and the current list of citations canonical bibcodes
    """
    for curated_entry in curated_entries:
        updated = False
        bibcode_replaced = {}
        
        #Try by doi.
        if curated_entry.get('doi'):
            registered_record = db.get_citation_targets_by_doi(app, [curated_entry.get('doi')], only_status='REGISTERED')[0]           
        #If not, retrieve entry by bibcode.
        elif curated_entry.get('bibcode'):
            registered_record = db.get_citation_targets_by_bibcode(app, [curated_entry.get('bibcode')], only_status='REGISTERED')[0]   
        #report error
        else:
            logger.error('Unable to retrieve entry for {} from database. Please check input file.'.format(curated_entry))
        
        metadata = db.get_citation_target_metadata(app, registered_record.get('content', ''), curate = False)
        raw_metadata = metadata.get('raw', '')
        parsed_metadata = metadata.get('parsed', '')
        #remove doi and bibcode from metadata to be stored in db.
        for key in ['bibcode','doi']:
            try:
                curated_entry.pop(key)
            except KeyError as e:
                logger.error("Failed to remove key: {} with error {}".format(key, e))
                continue
        try:
            if not reset:
                #only check old metadata if we are adding updates, otherwise ignore.
                for  key in registered_record['curated_metadata'].keys():
                    #first apply any previous edits to metadata that are not overwritten by new metadata.
                    if key not in curated_entry.keys():
                        curated_entry[key] = registered_record['curated_metadata'][key]

                modified_metadata = db.generate_modified_metadata(parsed_metadata, curated_entry)
            
                #regenerate bibcode with curated_metadata and append old bibcode to alternate_bibcode 
                zenodo_bibstem = "zndo"
                new_bibcode = doi.build_bibcode(modified_metadata, doi.zenodo_doi_re, zenodo_bibstem)
                alternate_bibcode = registered_record.get('alternate_bibcode', [])
                parsed_metadata['alternate_bibcode'] = registered_record.get('alternate_bibcode', [])
                if 'alternate_bibcode' in curated_entry.keys():
                    alternate_bibcode = list(set(alternate_bibcode+curated_entry['alternate_bibcode']))
                    logger.debug('alternate bibcodes are {}'.format(alternate_bibcode))
                if new_bibcode != registered_record.get('bibcode'):
                    logger.warn("Parsing the new metadata for citation target '%s' produced a different bibcode: '%s'. The former will be moved to the 'alternate_bibcode' list, and the new one will be used as the main one.", registered_record['bibcode'],new_bibcode)
                    if registered_record.get('bibcode') not in alternate_bibcode:
                        #generate complete alt bibcode list including any curated entries
                        alternate_bibcode.append(registered_record.get('bibcode'))
                        #Add the CC generated bibcode to the parsed metadata
                        parsed_metadata['alternate_bibcode'].append(registered_record.get('bibcode'))
                parsed_metadata['alternate_bibcode'] = list(set(parsed_metadata.get('alternate_bibcode')))
                modified_metadata['bibcode'] = new_bibcode
                bibcode_replaced = {'previous': registered_record['bibcode'], 'new': parsed_metadata['bibcode'] }
                alt_bibcodes = list(set(alternate_bibcode))
                modified_metadata['alternate_bibcode'] = alt_bibcodes
                curated_entry['alternate_bibcode'] = alt_bibcodes
                   
            else:
                #Repopulate parsed_metadata with expected bibcode information from parsed_cited_metadata.
                logger.debug("Resetting citation to original parsed metadata")
                #regenerate bibcode with parsed_metadata and append old bibcode to alternate_bibcode 
                zenodo_bibstem = "zndo"
                new_bibcode = doi.build_bibcode(parsed_metadata, doi.zenodo_doi_re, zenodo_bibstem)
                alternate_bibcode = registered_record.get('alternate_bibcode', [])
                parsed_metadata['alternate_bibcode'] = registered_record.get('alternate_bibcode', [])
                if new_bibcode != registered_record.get('bibcode'):
                    logger.warn("Parsing the new metadata for citation target '%s' produced a different bibcode: '%s'. The former will be moved to the 'alternate_bibcode' list, and the new one will be used as the main one.", registered_record['bibcode'],new_bibcode)
                    if registered_record.get('bibcode') not in alternate_bibcode:
                        alternate_bibcode.append(registered_record.get('bibcode'))
                parsed_metadata['alternate_bibcode'] = list(set(alternate_bibcode))
                bibcode_replaced = {'previous': registered_record['bibcode'], 'new': parsed_metadata['bibcode'] }
                modified_metadata = parsed_metadata
                curated_entry = {}
            
            different_bibcodes = registered_record['bibcode'] != modified_metadata['bibcode']
            if different_bibcodes:
                event_data = webhook.identical_bibcodes_event_data(registered_record['bibcode'], modified_metadata['bibcode'])
                if event_data:
                    dump_prefix = datetime.now().strftime("%Y%m%d") # "%Y%m%d_%H%M%S"
                    logger.debug("Calling 'task_emit_event' for '%s' IsIdenticalTo '%s'", registered_record['bibcode'], modified_metadata['bibcode'])
                    task_emit_event.delay(event_data, dump_prefix)
                
            updated = db.update_citation_target_metadata(app, registered_record['content'], raw_metadata, parsed_metadata, curated_metadata = curated_entry, bibcode = modified_metadata.get('bibcode'))
            if updated:
                citation_change = adsmsg.CitationChange(content=registered_record['content'],
                                                            content_type=getattr(adsmsg.CitationChangeContentType, registered_record['content_type'].lower()),
                                                            status=adsmsg.Status.updated,
                                                            timestamp=datetime.now()
                                                            )
                if citation_change.content_type == adsmsg.CitationChangeContentType.doi:
                    # Get citations from the database and transform the stored bibcodes into their canonical ones as registered in Solr.
                    original_citations = db.get_citations_by_bibcode(app, registered_record['bibcode'])
                    citations = api.get_canonical_bibcodes(app, original_citations)
                    logger.debug("Calling 'task_output_results' with '%s'", citation_change)
                    task_output_results.delay(citation_change, modified_metadata, citations, bibcode_replaced=bibcode_replaced)
        
        except Exception as e:
            logger.error("task_maintenance_curation Failed to update metadata for {} with Exception: {}. Please check that the bibcode or doi matches a target record.".format(curated_entry, e))
            raise

def maintenance_show_metadata(curated_entries):
    """
    Print current metadata for a given citation target to standard output.
    """
    for curated_entry in curated_entries:

        if curated_entry.get('doi'):
            try:
                registered_record = db.get_citation_targets_by_doi(app, [curated_entry.get('doi')], only_status='REGISTERED')[0]   
            except Exception as e:
                msg = "Failed to retrieve citation target {}. Please confirm information is correct and citation target is in database.".format(curated_entry)
                logger.error(msg)
                raise Exception(msg)

            custom_citation_change = adsmsg.CitationChange(content=registered_record['content'],
                                                    content_type=getattr(adsmsg.CitationChangeContentType, registered_record['content_type'].lower()),
                                                    status=adsmsg.Status.updated,
                                                    timestamp=datetime.now()
                                                    )
            try:
                parsed_metadata = db.get_citation_target_metadata(app, custom_citation_change.content).get('parsed', None)
                if parsed_metadata:
                    print(parsed_metadata)

            except Exception as e:
                msg = "Failed to load metadata for citation {}. Please confirm information is correct and citation target is in database.".format(curated_entry)
                logger.error(msg)
        
        #If no doi, try and retrieve entry by bibcode.
        elif curated_entry.get('bibcode'):
            try:
                registered_record = db.get_citation_targets_by_bibcode(app, [curated_entry.get('bibcode')], only_status='REGISTERED')[0]   
            except Exception as e:
                msg = "Failed to retrieve citation target {}. Please confirm information is correct and citation target is in database.".format(curated_entry)
                logger.error(msg)
                raise Exception(msg)

            custom_citation_change = adsmsg.CitationChange(content=registered_record['content'],
                                                    content_type=getattr(adsmsg.CitationChangeContentType, registered_record['content_type'].lower()),
                                                    status=adsmsg.Status.updated,
                                                    timestamp=datetime.now()
                                                    )
            parsed_metadata = db.get_citation_target_metadata(app, custom_citation_change.content).get('parsed', None)
            try:
                parsed_metadata = db.get_citation_target_metadata(app, custom_citation_change.content).get('parsed', None)
                if parsed_metadata:
                    print(parsed_metadata)

            except Exception as e:
                msg = "Failed to load metadata for citation {}. Please confirm information is correct and citation target is in database.".format(curated_entry)
                logger.error(msg)

@app.task(queue='maintenance_metadata')
def task_maintenance_repopulate_bibcode_columns(curated = True):
    """
    Re-populates bibcode column with current canonical bibcode
    """
    db.populate_bibcode_column(app, curated)

@app.task(queue='maintenance_resend')
def task_maintenance_resend(dois, bibcodes, broker):
    """
    Maintenance operation:
    - Get all the registered citation targets (or only a subset of them if DOIs and/or bibcodes are specified)
    - For each:
        - Re-send to master (or broker) an update with the current metadata and the current list of citations canonical bibcodes
    """
    n_requested = len(dois) + len(bibcodes)
    if n_requested == 0:
        registered_records = db.get_citation_targets(app, only_status='REGISTERED')
        if broker:
            emittable_records = db.get_citation_targets(app, only_status='EMITTABLE')
        else:
            emittable_records=[]
    else:
        registered_records = db.get_citation_targets_by_bibcode(app, bibcodes, only_status='REGISTERED')
        registered_records += db.get_citation_targets_by_doi(app, dois, only_status='REGISTERED')
        registered_records = _remove_duplicated_dict_in_list(registered_records)

        if broker:
            emittable_records = db.get_citation_targets_by_bibcode(app, bibcodes, only_status='EMITTABLE')
            emittable_records = _remove_duplicated_dict_in_list(emittable_records)
        else:
            emittable_records = []

    for registered_record in registered_records:
        citations = db.get_citations_by_bibcode(app, registered_record['bibcode'])
        custom_citation_change = adsmsg.CitationChange(content=registered_record['content'],
                                                       content_type=getattr(adsmsg.CitationChangeContentType, registered_record['content_type'].lower()),
                                                       status=adsmsg.Status.updated,
                                                       timestamp=datetime.now()
                                                       )
        parsed_metadata = db.get_citation_target_metadata(app, custom_citation_change.content).get('parsed', {})
        if parsed_metadata:
            if not broker:
                # Only update master
                logger.debug("Calling 'task_output_results' with '%s'", custom_citation_change)
                task_output_results.delay(custom_citation_change, parsed_metadata, citations)
            else:
                # Only re-emit to the broker
                # Signal that the target bibcode and the DOI are identical
                event_data = webhook.identical_bibcode_and_doi_event_data(registered_record['bibcode'], registered_record['content'])
                if event_data:
                    dump_prefix = custom_citation_change.timestamp.ToDatetime().strftime("%Y%m%d_%H%M%S_resent")
                    logger.debug("Calling 'task_emit_event' for '%s' IsIdenticalTo '%s'", registered_record['bibcode'], registered_record['content'])
                    task_emit_event.delay(event_data, dump_prefix)
                # And for each citing bibcode to the target DOI
                for citing_bibcode in citations:
                    emit_citation_change = adsmsg.CitationChange(citing=citing_bibcode,
                                                                   content=registered_record['content'],
                                                                   content_type=getattr(adsmsg.CitationChangeContentType, registered_record['content_type'].lower()),
                                                                   status=adsmsg.Status.new,
                                                                   timestamp=datetime.now()
                                                                   )
                    # Signal that the citing bibcode cites the DOI
                    event_data = webhook.citation_change_to_event_data(emit_citation_change, parsed_metadata)
                    if event_data:
                        dump_prefix = emit_citation_change.timestamp.ToDatetime().strftime("%Y%m%d_%H%M%S_resent")
                        logger.debug("Calling 'task_emit_event' for '%s'", emit_citation_change)
                        task_emit_event.delay(event_data, dump_prefix)
    if broker:
        for emittable_record in emittable_records:
            citations = db.get_citations_by_bibcode(app, emittable_record['bibcode'])
            custom_citation_change = adsmsg.CitationChange(content=emittable_record['content'],
                                                        content_type=getattr(adsmsg.CitationChangeContentType, emittable_record['content_type'].lower()),
                                                        status=adsmsg.Status.updated,
                                                        timestamp=datetime.now()
                                                        )
            parsed_metadata = db.get_citation_target_metadata(app, custom_citation_change.content).get('parsed', {})
            if parsed_metadata:
                # And for each citing bibcode to the target DOI
                for citing_bibcode in citations:
                    emit_citation_change = adsmsg.CitationChange(citing=citing_bibcode,
                                                                content=emittable_record['content'],
                                                                content_type=getattr(adsmsg.CitationChangeContentType, emittable_record['content_type'].lower()),
                                                                status=adsmsg.Status.new,
                                                                timestamp=datetime.now()
                                                                )
                    # Signal that the citing bibcode cites the DOI
                    event_data = webhook.citation_change_to_event_data(emit_citation_change, parsed_metadata)
                    if event_data:
                        dump_prefix = emit_citation_change.timestamp.ToDatetime().strftime("%Y%m%d_%H%M%S_resent")
                        logger.debug("Calling 'task_emit_event' for '%s'", emit_citation_change)
                        task_emit_event.delay(event_data, dump_prefix)


@app.task(queue='maintenance_reevaluate')
def task_maintenance_reevaluate(dois, bibcodes):
    """
    Maintenance operation:
    - Get all the registered citation targets (or only a subset of them if DOIs and/or bibcodes are specified)
    - For each, retreive metadata and if it is different to what we have in our database:
        - Get the citations bibcodes and transform them to their canonical form
        - Send to master an update with the new metadata and the current list of citations canonical bibcodes
    """
    n_requested = len(dois) + len(bibcodes)
    if n_requested == 0:
        discarded_records = db.get_citation_targets(app, only_status='DISCARDED')
    else:
        discarded_records = db.get_citation_targets_by_bibcode(app, bibcodes, only_status='DISCARDED')
        discarded_records += db.get_citation_targets_by_doi(app, dois, only_status='DISCARDED')
        discarded_records = _remove_duplicated_dict_in_list(discarded_records)

    for previously_discarded_record in discarded_records:
        updated = False
        bibcode_replaced = {}
        # Fetch DOI metadata (if HTTP request fails, an exception is raised
        # and the task will be re-queued (see app.py and adsputils))
        if previously_discarded_record['content_type'] == 'DOI':
            raw_metadata = doi.fetch_metadata(app.conf['DOI_URL'], app.conf['DATACITE_URL'], previously_discarded_record['content'])
            if raw_metadata:
                parsed_metadata = doi.parse_metadata(raw_metadata)
                is_software = parsed_metadata.get('doctype', '').lower() == "software"
                if not is_software:
                    logger.error("Discarded '%s', it is not 'software'", previously_discarded_record['content'])
                elif parsed_metadata.get('bibcode') in (None, ""):
                    logger.error("The metadata for '%s' could not be parsed correctly and it did not correctly compute a bibcode", previously_discarded_record['content'])
                else:
                    # Create citation target in the DB
                    updated = db.update_citation_target_metadata(app, previously_discarded_record['content'], raw_metadata, parsed_metadata, status='REGISTERED')
                    if updated:
                        db.mark_all_discarded_citations_as_registered(app, previously_discarded_record['content'])
            if updated:
                citation_change = adsmsg.CitationChange(content=previously_discarded_record['content'],
                                                            content_type=getattr(adsmsg.CitationChangeContentType, previously_discarded_record['content_type'].lower()),
                                                            status=adsmsg.Status.new,
                                                            timestamp=datetime.now()
                                                            )
                if citation_change.content_type == adsmsg.CitationChangeContentType.doi:
                    # Get citations from the database and transform the stored bibcodes into their canonical ones as registered in Solr.
                    original_citations = db.get_citations_by_bibcode(app, parsed_metadata['bibcode'])
                    citations = api.get_canonical_bibcodes(app, original_citations)
                    logger.debug("Calling 'task_output_results' with '%s'", citation_change)
                    task_output_results.delay(citation_change, parsed_metadata, citations, bibcode_replaced=bibcode_replaced)
        raw_metadata = doi.fetch_metadata(app.conf['DOI_URL'], app.conf['DATACITE_URL'], previously_discarded_record['content'])

@app.task(queue='maintenance_associated_works')
def task_maintenance_reevaluate_associated_works(dois, bibcodes):
    """
    Maintenance operation:
    - Get all the registered citation targets (or only a subset of them if DOIs and/or bibcodes are specified)
    - For each, retreive metadata and:
        - Get associated works from metadata.
        - Determine which associated works are currently in the db.
        - Send updates to master with the added associated works.
    """
    n_requested = len(dois) + len(bibcodes)
    if n_requested == 0:
        registered_records = db.get_citation_targets(app, only_status='REGISTERED')
    else:
        registered_records = db.get_citation_targets_by_bibcode(app, bibcodes, only_status='REGISTERED')
        registered_records += db.get_citation_targets_by_doi(app, dois, only_status='REGISTERED')
        registered_records = _remove_duplicated_dict_in_list(registered_records)

    for registered_record in registered_records:
        citations = db.get_citations_by_bibcode(app, registered_record['bibcode'])
        custom_citation_change = adsmsg.CitationChange(content=registered_record['content'],
                                                       content_type=getattr(adsmsg.CitationChangeContentType, registered_record['content_type'].lower()),
                                                       status=adsmsg.Status.updated,
                                                       timestamp=datetime.now()
                                                       )
        raw_metadata = doi.fetch_metadata(app.conf['DOI_URL'], app.conf['DATACITE_URL'], registered_record['content'])
        if raw_metadata:
            parsed_metadata = doi.parse_metadata(raw_metadata)
            is_software = parsed_metadata.get('doctype', '').lower() == "software"
            if not is_software:
                logger.error("Discarded '%s', it is not 'software'", registered_record['content'])
            elif parsed_metadata.get('bibcode') in (None, ""):
                logger.error("The metadata for '%s' could not be parsed correctly and it did not correctly compute a bibcode", registered_record['content'])
            else:
                logger.debug("Calling 'task_output_results' with '%s'", custom_citation_change)
                    #Check for additional versions
                try:
                    all_versions_doi = doi.fetch_all_versions_doi(app.conf['DOI_URL'], app.conf['DATACITE_URL'], parsed_metadata)
                except:
                    logger.error("Unable to recover related versions for {}",custom_citation_change)
                    all_versions_doi = None
                #fetch additional versions from db if they exist.
                if all_versions_doi['versions'] not in (None,[]):
                    logger.info("Found {} versions for {}".format(len(all_versions_doi['versions']),custom_citation_change.content))
                    versions_in_db=db.get_associated_works_by_doi(app, all_versions_doi)
                    #Only add bibcodes if there are versions in db, otherwise leave as None.
                    if versions_in_db not in (None, [None]):
                        logger.info("Found {} versions in database for {}".format(len(versions_in_db),custom_citation_change.content))
                        #adds the new citation target bibcode because it will not be in the db yet, 
                        # and then appends the versions already in the db.
                        associated_version_bibcodes=versions_in_db
                        logger.debug("{}: associated_versions_bibcodes".format(associated_version_bibcodes))
                        task_process_updated_associated_works.delay(registered_record['bibcode'], associated_version_bibcodes)
                    

@app.task(queue='output-results')
def task_output_results(citation_change, parsed_metadata, citations, db_versions={"":""}, bibcode_replaced={}):
    """
    This worker will forward results to the outside
    exchange (typically an ADSMasterPipeline) to be
    incorporated into the storage

    :param citation_change: contains citation changes
    :return: no return
    """
    try:
        entry_date = db.get_citation_target_entry_date(app, citation_change.content)
    except:
        try:
            entry_date = db.get_citation_target_entry_date(app, citation_change['content'])
        except Exception as e:
            logger.error("Failed to retrieve entry date for {}".format(citation_change))

    messages = []
    if bibcode_replaced:
        # Bibcode was replaced, this is not a simple update
        # we need to issue a deletion of the previous record
        custom_citation_change = adsmsg.CitationChange(content=citation_change.content,
                                                       content_type=citation_change.content_type,
                                                       status=adsmsg.Status.deleted,
                                                       timestamp=datetime.now()
                                                       )
        delete_parsed_metadata = parsed_metadata.copy()
        delete_parsed_metadata['bibcode'] = bibcode_replaced['previous']
        delete_parsed_metadata['alternate_bibcode'] = [x for x in delete_parsed_metadata.get('alternate_bibcode', []) if x not in (bibcode_replaced['previous'], bibcode_replaced['new'])]
        delete_record, delete_nonbib_record = forward.build_record(app, custom_citation_change, delete_parsed_metadata, citations, entry_date=entry_date)
        messages.append((delete_record, delete_nonbib_record))
    # Main message:
    record, nonbib_record = forward.build_record(app, citation_change, parsed_metadata, citations, db_versions, entry_date=entry_date)
    messages.append((record, nonbib_record))

    for record, nonbib_record in messages:
        logger.debug('Will forward this record: %s', record)
        logger.debug("Calling 'app.forward_message' with '%s'", str(record.toJSON()))
        if not app.conf['CELERY_ALWAYS_EAGER']:
            app.forward_message(record)
        logger.debug('Will forward this record: %s', nonbib_record)
        logger.debug("Calling 'app.forward_message' with '%s'", str(nonbib_record.toJSON()))
        if not app.conf['CELERY_ALWAYS_EAGER']:
            app.forward_message(nonbib_record)


if __name__ == '__main__':
    app.start()
