from json_loader import generate_json
from json_loader import print_structure
import json
from sql_executor import SqlBuilder


if __name__ == '__main__':
    res = json.loads(generate_json('73', '905'))
    print_structure(res)
    print(res)
