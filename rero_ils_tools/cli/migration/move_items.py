# -*- coding: utf-8 -*-
#
# Copyright (C) 2021 UCLouvain.
#

"""Click command-line utilities."""

from __future__ import absolute_import, print_function

import click
import sys
from flask.cli import with_appcontext
from pprint import pprint
from datetime import date, datetime, time

from rero_ils.modules.utils import get_ref_for_pid, JsonWriter
from rero_ils.modules.documents.api import Document, DocumentsIndexer
from rero_ils.modules.holdings.api import Holding
from rero_ils.modules.item_types.api import ItemType
from rero_ils.modules.items.api import Item, ItemsSearch
from rero_ils.modules.locations.api import Location


@click.command("move_items_location")
@click.option('-l', '--loc-pid', 'loc_pid',
              help='pid of the location')
@click.option('-i', '--itty-pid', 'itty_pid',
              help='pid of the item type')
@click.option('-f', '--file', 'infile', type=click.File('r'),
              help='item pids file')
@click.option('-q', '--query', 'query_string',
              help='elasticsearch query string')
@click.option('-v', '--verbose', 'verbose', is_flag=True, default=False,
              help='display more information')
@click.option('--yes-i-know', 'confirm_do_change', is_flag=True,
              default=False, help='confirm change (write to DB)')
@with_appcontext
def move_items_location(infile, query_string, loc_pid, itty_pid,
                        confirm_do_change, verbose):
    """Update location and / or item type in items records.

    requirement:
        - the location must exist
        - the item type must exist

    workflow:
        1. update and reindex items
        2. reindex holdings
        3. reindex documents
    """
    start_time = datetime.now()
    click.secho(f'MOVE ITEMS TO LOCATION',
                fg='red', underline=True, bold=True)

    document_pids = []
    item_pids = []
    holding_pids = []
    items_on_loan = []
    items_on_shelf = []
    nb_item_updated = 0
    nb_item_error = 0
    errors = []
    err_file_name = 'move_items_errors.log'
    if not loc_pid:
        click.secho('There are missing required arguments! \n'
                    'please check target location pid ', fg='yellow')
        sys.exit()
    if infile and not query_string:
        item_pids = [pid.strip() for pid in infile]

    if query_string and not infile:
        search = ItemsSearch().query(
            'query_string', query=query_string).source('pid')
        item_pids = [hit.pid for hit in search.scan()]

    if not item_pids:
        click.secho('There are no items to process...', fg='yellow')
        sys.exit()

    # get location
    target_loc = Location.get_record_by_pid(loc_pid)

    # get library
    target_library = target_loc.get_library()
    target_lib_ref = get_ref_for_pid('lib', target_library.pid)
    target_loc_ref = get_ref_for_pid('loc', loc_pid)

    itty = None
    if itty_pid:
        itty = ItemType.get_record_by_pid(itty_pid)
        itty_ref = get_ref_for_pid('itty', target_library.pid)
    if query_string:
        click.secho(f' -> query_string: {query_string}', fg='yellow')

    # STEP-1: move items to new location pid =================================
    click.secho('[STEP-1] Update location and / or item type in items records',
                fg='white', bold=True)

    with click.progressbar(item_pids, length=len(item_pids)) as bar:
        for pid in bar:
            try:
                item_touched = False
                item = Item.get_record_by_pid(pid)
                # retrieve holding pid for update record in step 3
                holding_pids.append(item.holding_pid)
                # retrieve document pid for reindexing in step 5
                document_pids.append(item.document_pid)
                if verbose:
                    click.secho('[ITEM] before update', fg='white', bold=True)
                    pprint(item)
                # update item type
                if itty:
                    item['item_type']['$ref'] = itty_ref
                    item_touched = True
                # update location
                if loc_pid and item['location']['$ref'] != target_loc_ref:
                    item['location']['$ref'] = target_loc_ref
                    item['library']['$ref'] = target_lib_ref
                    item_touched = True
                if verbose:
                    click.secho('[ITEM] after update', fg='white',
                                bold=True)
                    pprint(item)
                if item_touched and confirm_do_change:
                    item.update(item, dbcommit=True, commit=True,
                                reindex=True)

                    nb_item_updated += 1
                    if item.get('status') == 'on_loan':
                        items_on_loan.append(pid)
                    else:
                        items_on_shelf.append(pid)

            except Exception as err:
                errors.append({
                    'type': 'item',
                    'pid': pid,
                    'error': str(err)
                })
                nb_item_error += 1
    click.secho(f'=> {nb_item_updated} items moved',
                fg='green')
    if nb_item_error:
        click.secho(f'=> {nb_item_error} items on error',
                    fg='red')

    # STEP-2: reindex holdings ================================================
    click.secho('[STEP-2] reindex holdings',
                fg='white', bold=True)
    nb_holding_updated = 0
    nb_holding_error = 0
    with click.progressbar(holding_pids, length=len(holding_pids)) as bar:
        for pid in bar:
            try:
                holding = Holding.get_record_by_pid(pid)
                if confirm_do_change:
                    holding.reindex()
            except Exception as err:
                errors.append({
                    'type': 'hold',
                    'pid': pid,
                    'error': str(err)
                })
                nb_holding_error += 1
    click.secho(f'=> {nb_holding_updated} holdings reindexed', fg='green')
    if nb_item_error:
        click.secho(f'=> {nb_holding_error} holdings on error', fg='red')

    # STEP-3: reindex documents ==============================================
    click.secho('[STEP-3] Bulk index document records',
                fg='white', bold=True)
    document_pids = set(list(sorted(document_pids)))
    documents_count = len(document_pids)
    click.secho(f'=> found {documents_count} documents to reindex', fg='blue')

    document_ids = []
    nb_document_error = 0
    with click.progressbar(document_pids, length=documents_count) as bar:
        for document_pid in bar:
            try:
                document_ids.append(str(Document.get_id_by_pid(document_pid)))
            except Exception as err:
                errors.append({
                    'type': 'doc',
                    'pid': document_pid,
                    'error': str(err)
                })
                nb_document_error += 1
        # do bulk index
        if confirm_do_change:
            DocumentsIndexer().bulk_index(document_ids)
    click.secho(f'=> {len(document_ids)} documents queued for indexing',
                fg='green')

    # Write error on disk =====================================================
    if errors:
        err_file_name = 'move_location_errors.log'
        click.secho(f'[STEP-4] Write error file: {err_file_name}',
                    fg='blue')
        error_file = JsonWriter(err_file_name)
        for error in errors:
            error_file.write(error)

    click.secho('\nSummary:', fg='white', underline=True, bold=True)
    click.echo('{label}: {value}'.format(
        label=click.style('elapsed time', fg='green'),
        value=click.style(str(datetime.now() - start_time), fg="yellow")))
    if query_string:
        click.echo('{label}: {value}'.format(
            label=click.style('query string', fg='green'),
            value=click.style(str(query_string), fg="yellow")))
    click.echo('{label}: {value}'.format(
        label=click.style('total items', fg='green'),
        value=click.style(str(len(item_pids)), fg="yellow")))
    click.echo('{label}: {value}'.format(
        label=click.style(' => items status on shelf', fg='green'),
        value=click.style(str(len(items_on_shelf)), fg="yellow")))
    click.echo('{label}: {value}'.format(
        label=click.style(' => items status on loans', fg='green'),
        value=click.style(str(len(items_on_loan)), fg="yellow")))
    click.echo('{label}: {value}'.format(
        label=click.style(' => items moved', fg='green'),
        value=click.style(str(nb_item_updated), fg="yellow")))
    click.echo('{label}: {value}'.format(
        label=click.style('total holdings', fg='green'),
        value=click.style(str(len(holding_pids)), fg="yellow")))
    click.echo('{label}: {value}'.format(
        label=click.style(' => holdings reindexed', fg='green'),
        value=click.style(str(nb_holding_updated), fg="yellow")))
    click.echo('{label}: {value}'.format(
        label=click.style('total documents', fg='green'),
        value=click.style(str(len(document_pids)), fg="yellow")))
    click.echo('{label}: {value}'.format(
        label=click.style('documents to reindex', fg='green'),
        value=click.style(str(len(document_ids)), fg="yellow")))
    click.secho('\nError:', fg='red', underline=True, bold=True)

    color = 'yellow'
    if errors:
        color = 'red'
    click.echo('{label}: {value}'.format(
        label=click.style(' => total', fg='green'),
        value=click.style(str(len(errors)), fg=color)))
    click.echo('{label}: {value}'.format(
        label=click.style(' => items', fg='green'),
        value=click.style(str(nb_item_error), fg=color)))
    click.echo('{label}: {value}'.format(
        label=click.style(' => holdings', fg='green'),
        value=click.style(str(nb_holding_error), fg=color)))
    click.echo('{label}: {value}'.format(
        label=click.style(' => documents', fg='green'),
        value=click.style(str(nb_document_error), fg=color)))

    if errors:
        click.secho(f'!!! PLEASE READ ERROR FILE: {err_file_name} !!!',
                    fg='white', bg='red', bold=True, blink=True)
