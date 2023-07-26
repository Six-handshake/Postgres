from json_loader import generate_json, generate_for_single
from json_loader import print_structure
import json


if __name__ == '__main__':
    # res = json.loads(generate_json('73', '850'))
    res = json.loads(generate_for_single('73'))
    print_structure(res)
    print(res)
