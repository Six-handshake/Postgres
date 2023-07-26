import json

from ssh_tests import get_links_via_ssh, get_all_links_via_ssh
from postgres_service import get_links, get_all_links


def generate_json(le1: str, le2: str):
    res = get_links(le1, le2)
    # res = get_links_via_ssh(le1, le2)
    return json.dumps(res)


def generate_for_single(le: str):
    # res = get_all_links(le)
    res = get_all_links_via_ssh(le)
    return json.dumps(res)


def print_structure(links: list):
    if links is None:
        return

    current_depth = -1
    les = dict()
    for e in links:
        if e['depth'] != current_depth:
            current_depth = e['depth']
            print_children(les, current_depth)
            les = dict()
            print(f'\n\nDepth: {current_depth}\n')
        if e['child'] not in les:
            les[e['child']] = []
        les[e['child']].append(e)
    current_depth += 1
    print_children(les, current_depth)


def print_children(d: dict, current_depth):
    colors = ['\033[95m', '\033[94m', '\033[96m', '\033[92m', '\033[93m', '\033[91m']
    endcolor = '\033[0m'

    for le in d:
        print(f'{le}: ' + ", ".join(
            map(lambda x: colors[current_depth % len(colors)] + x['parent'] + (
                '(p)' if x['links'][0]['type'] == 'parent' else '(c)') +
                          endcolor if len(x['links']) else x['parent'],
                d[le])))
