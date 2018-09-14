#!/usr/bin/env python
import os
import sys
import tempfile
import argparse
import json
from ADSCitationCapture import tasks

from adsputils import setup_logging
logger = setup_logging('run.py')

from ADSCitationCapture.delta_computation import DeltaComputation
from adsputils import load_config
config = load_config()


def run(refids_filename, **kwargs):
    """
    Process file specified by the user.

    :param refids_filename: path to the file containing the citations
    :param kwargs: extra keyword arguments
    :return: no return
    """

    logger.info('Loading records from: {0}'.format(refids_filename))

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
        tasks.task_process_citation_changes.delay(changes)
    if diagnose:
        delta._execute_sql("drop schema {0} cascade;", delta.schema_name)


def build_diagnostics(bibcodes=None, json_payloads=None):
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

    parser = argparse.ArgumentParser(description='Process user input.')

    parser.add_argument('-r',
                        '--refids',
                        dest='refids',
                        action='store',
                        type=str,
                        help='Path to the refids.dat file'
                             ' that contains the citation list.')

    parser.add_argument('-f',
                        '--force',
                        dest='force',
                        action='store_true',
                        help='Force the processing of all the citations')

    parser.add_argument('-d',
                        '--diagnose',
                        dest='diagnose',
                        action='store_true',
                        default=False,
                        help='Process specific diagnostic default data')

    parser.add_argument('-b',
                        '--bibcodes',
                        dest='bibcodes',
                        action='store',
                        default=None,
                        help='Comma delimited list of bibcodes (for diagnostics)')

    parser.add_argument('-j',
                        '--json',
                        dest='json',
                        action='store',
                        default=None,
                        help='Semicolon delimited list of json citation (for diagnostics)')

    parser.set_defaults(refids=False)
    parser.set_defaults(force=False)
    parser.set_defaults(diagnose=False)

    args = parser.parse_args()

    if args.diagnose:
        if args.bibcodes:
            args.bibcodes = [x.strip() for x in args.bibcodes.split(',')]
        else:
            # Defaults
            args.bibcodes = ["1005PhRvC..71c4906H", "1915PA.....23..189P", "2017PASP..129b4005R"]

        if args.json:
            args.json = [x.strip() for x in args.json.split(';')]
        else:
            # Defaults
            args.json = [
                    "{\"cited\":\"1976NuPhB.113..395J\",\"citing\":\"1005PhRvC..71c4906H\",\"doi\":\"10.1016/0550-3213(76)90133-4\",\"score\":\"1\",\"source\":\"/proj/ads/references/resolved/PhRvC/0071/1005PhRvC..71c4906H.ref.xml.result:17\"}",
                    "{\"cited\":\"...................\",\"citing\":\"2017SSEle.128..141M\",\"score\":\"0\",\"source\":\"/proj/ads/references/resolved/SSEle/0128/10.1016_j.sse.2016.10.029.xref.xml.result:10\",\"url\":\"https://github.com/viennats/viennats-dev\"}",
                    "{\"cited\":\"2013ascl.soft03021B\",\"citing\":\"2017PASP..129b4005R\",\"pid\":\"ascl:1303.021\",\"score\":\"1\",\"source\":\"/proj/ads/references/resolved/PASP/0129/iss972.iop.xml.result:114\"}",
                    ]

        args.refids = build_diagnostics(json_payloads=args.json, bibcodes=args.bibcodes)

    if not args.refids:
        print 'You need to give the input list'
        parser.print_help()
        sys.exit(0)

    # Send the files to be put on the queue
    run(args.refids,
        force=args.force,
        diagnose=args.diagnose)

    if args.diagnose:
        print("Removing diagnostics temporary file '{}'".format(args.refids))
        os.unlink(args.refids)
