from json_loader import generate_json
from json_loader import print_structure
import json


if __name__ == '__main__':
    res = json.loads(generate_json('73', '713'))
    print_structure(res)
    print(res)
