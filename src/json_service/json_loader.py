from postgres_srv import ssh_tests
from postgres_srv import get_links

def generate_json():
    print(get_links('73', '76'))


if __name__ == '__main__':
    generate_json()
