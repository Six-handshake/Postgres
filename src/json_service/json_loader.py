import json

from src.postgres_srv import get_links_via_ssh
from src.postgres_srv import get_links


def generate_json(le1: str, le2: str):
    res = get_links(le1, le2)
    # print_structure(res)
    return json.dumps(res)


def print_structure(links: list):
    if links is None:
        return

    colors = ['\033[95m', '\033[94m', '\033[96m', '\033[92m', '\033[93m', '\033[91m']
    endcolor = '\033[0m'

    prev_parents = []
    current_depth = -1
    les = dict()
    for e in links:
        if e['depth'] != current_depth:
            current_depth = e['depth']
            current_parents = []
            for le in les:
                current_parents.extend(les[le])
                print(f'{le}: {", ".join(map(lambda x: colors[current_depth % len(colors)] + x + endcolor if x in prev_parents else x, les[le]))}')
            les = dict()
            prev_parents = current_parents
            print(f'\n\nDepth: {current_depth}\n')
        if e['child'] not in les:
            les[e['child']] = []
        les[e['child']].append(e['parent'])
    current_depth += 1
    for le in les:
        print(f'{le}: {", ".join(map(lambda x: colors[current_depth % len(colors)] + x + endcolor if x in prev_parents else x, les[le]))}')


if __name__ == '__main__':
    print(generate_json('73', '76'))
