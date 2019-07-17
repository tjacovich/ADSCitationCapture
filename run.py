#!/usr/bin/env python
import os
import sys
import tempfile
import argparse
import json
from ADSCitationCapture import tasks, db

from adsputils import setup_logging
logger = setup_logging('run.py')
logger.propagate = False

from ADSCitationCapture.delta_computation import DeltaComputation
from adsputils import load_config
config = load_config()


def process(refids_filename, **kwargs):
    """
    Process file specified by the user.

    :param refids_filename: path to the file containing the citations
    :param kwargs: extra keyword arguments
    :return: no return
    """

    logger.info('Loading records from: %s', refids_filename)

    force = kwargs.get('force', False)
    diagnose = kwargs.get('diagnose', False)
    if diagnose:
        schema_prefix = "diagnose_citation_capture_"
    else:
        schema_prefix = kwargs.get('schema_prefix', "citation_capture_")

    # Engine
    sqlachemy_url = config.get('SQLALCHEMY_URL', 'postgres://user:password@localhost:5432/citation_capture_pipeline')
    sqlalchemy_echo = config.get('SQLALCHEMY_ECHO', False)

    delta = DeltaComputation(sqlachemy_url, sqlalchemy_echo=sqlalchemy_echo, group_changes_in_chunks_of=1, schema_prefix=schema_prefix, force=force)
    delta.compute(refids_filename)
    for changes in delta:
        if diagnose:
            print("Calling 'task_process_citation_changes' with '{}'".format(str(changes)))
        logger.debug("Calling 'task_process_citation_changes' with '%s'", str(changes))
        try:
            tasks.task_process_citation_changes.delay(changes, force=force)
        except:
            # In asynchronous mode, no exception is expected
            # In synchronous mode (for debugging purposes), exception may happen (e.g., failures to fetch metadata)
            logger.exception('Exception produced while processing citation changes')
    if diagnose:
        delta._execute_sql("drop schema {0} cascade;", delta.schema_name)

def maintenance_canonical(dois, bibcodes):
    """
    Updates canonical bibcodes (e.g., arXiv bibcodes that were merged with publisher bibcodes)
    Records that do not have the status 'REGISTERED' in the database will not be updated
    """
    n_requested = len(dois) + len(bibcodes)
    if n_requested == 0:
        logger.info("MAINTENANCE task: requested an update of all the canonical bibcodes")
    else:
        logger.info("MAINTENANCE task: requested an update of '{}' canonical bibcodes".format(n_requested))

    # Send to master updated citation bibcodes in their canonical form
    tasks.task_maintenance_canonical.delay(dois, bibcodes)


def maintenance_metadata(dois, bibcodes):
    n_requested = len(dois) + len(bibcodes)
    if n_requested == 0:
        logger.info("MAINTENANCE task: requested a metadata update for all the registered records")
    else:
        logger.info("MAINTENANCE task: requested a metadata update for '{}' records".format(n_requested))

    # Send to master updated metadata
    tasks.task_maintenance_metadata.delay(dois, bibcodes)

def diagnose(bibcodes, json):
    citation_count = db.get_citation_count(tasks.app)
    citation_target_count = db.get_citation_target_count(tasks.app)
    if citation_count != 0 or citation_target_count != 0:
        logger.error("Diagnose aborted because the database already contains %s citations and %s citations targets (this is a protection against modifying a database in use)", citation_count, citation_target_count)
    else:
        if not bibcodes:
            bibcodes = ["1005PhRvC..71c4906H", "1915PA.....23..189P", "2017PASP..129b4005R"]
            logger.info('Using default bibcodes for diagnose:\n\t%s', "\n\t".join(bibcodes))

        if not json:
            json = [
                    "{\"cited\":\"1976NuPhB.113..395J\",\"citing\":\"1005PhRvC..71c4906H\",\"doi\":\"10.1016/0550-3213(76)90133-4\",\"score\":\"1\",\"source\":\"/proj/ads/references/resolved/PhRvC/0071/1005PhRvC..71c4906H.ref.xml.result:17\"}",
                    "{\"cited\":\"...................\",\"citing\":\"2017SSEle.128..141M\",\"score\":\"0\",\"source\":\"/proj/ads/references/resolved/SSEle/0128/10.1016_j.sse.2016.10.029.xref.xml.result:10\",\"url\":\"https://github.com/viennats/viennats-dev\"}",
                    "{\"cited\":\"2013ascl.soft03021B\",\"citing\":\"2017PASP..129b4005R\",\"pid\":\"ascl:1303.021\",\"score\":\"1\",\"source\":\"/proj/ads/references/resolved/PASP/0129/iss972.iop.xml.result:114\"}",
                    ]
            logger.info('Using default json data for diagnose:\n\t%s', "\n\t".join(json))

        input_filename = _build_diagnostics(json_payloads=json, bibcodes=bibcodes)

        # Process diagnostic data
        process(input_filename, force=False, diagnose=True)



def _build_diagnostics(bibcodes=None, json_payloads=None):
    """
    Builds a temporary file to be used for diagnostics.
    """
    tmp_file = tempfile.NamedTemporaryFile(delete=False)
    print("Preparing diagnostics temporary file '{}'...".format(tmp_file.name))
    for bibcode, json_payload in zip(bibcodes, json_payloads):
        tmp_str = '{}\t{}'.format(bibcode, json_payload)
        print("\t{}".format(tmp_str))
        tmp_file.write(tmp_str+"\n")
    tmp_file.close()
    os.utime(tmp_file.name, (0, 0)) # set the access and modified times to 19700101_000000
    return tmp_file.name

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help='commands', dest="action")
    process_parser = subparsers.add_parser('PROCESS', help='Process input file, compare to previous data in database, and execute insertion/deletions/updates of citations')
    process_parser.add_argument('input_filename',
                        action='store',
                        type=str,
                        help='Path to the input file (e.g., refids.dat) file that contains the citation list')
    maintenance_parser = subparsers.add_parser('MAINTENANCE', help='Execute maintenance task')
    maintenance_parser.add_argument(
                        '--canonical',
                        dest='canonical',
                        action='store_true',
                        default=False,
                        help='Update citations with canonical bibcodes')
    maintenance_parser.add_argument(
                        '--metadata',
                        dest='metadata',
                        action='store_true',
                        default=False,
                        help='Update DOI metadata for the provided list of citation target bibcodes, or if none is provided, for all the current existing citation targets')
    maintenance_parser.add_argument(
                        '--doi',
                        dest='dois',
                        nargs='+',
                        action='store',
                        default=[],
                        help='Space separated DOI list (e.g., 10.5281/zenodo.10598), if no list is provided then the full database is considered')
    maintenance_parser.add_argument(
                        '--bibcode',
                        dest='bibcodes',
                        nargs='+',
                        action='store',
                        default=[],
                        help='Space separated bibcode list, if no list is provided then the full database is considered')
    diagnose_parser = subparsers.add_parser('DIAGNOSE', help='Process data for diagnosing infrastructure')
    diagnose_parser.add_argument(
                        '--bibcodes',
                        dest='bibcodes',
                        nargs='+',
                        action='store',
                        default=None,
                        help='Space delimited list of bibcodes')
    diagnose_parser.add_argument(
                        '--json',
                        dest='json',
                        nargs='+',
                        action='store',
                        default=None,
                        help='Space delimited list of json citation data')

    args = parser.parse_args()

    if args.action == "PROCESS":
        if not os.path.exists(args.input_filename):
            process_parser.error("the file '{}' does not exist".format(args.input_filename))
        elif not os.access(args.input_filename, os.R_OK):
            process_parser.error("the file '{}' cannot be accessed".format(args.input_filename))
        else:
            logger.info("PROCESS task: %s", args.input_filename)
            process(args.input_filename, force=False, diagnose=False)
    elif args.action == "MAINTENANCE":
        if not args.canonical and not args.metadata:
            maintenance_parser.error("nothing to be done since no task has been selected")
        else:
            if args.metadata:
                maintenance_metadata(args.dois, args.bibcodes)
            elif args.canonical:
                maintenance_canonical(args.dois, args.bibcodes)
    elif args.action == "DIAGNOSE":
        logger.info("DIAGNOSE task")
        diagnose(args.bibcodes, args.json)
    else:
        raise Exception("Unknown argument action: {}".format(args.action))

