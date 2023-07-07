from postgres_srv import get_links_via_ssh
from postgres_srv import get_links

def generate_json():
    print(get_links_via_ssh('73', '76'))


if __name__ == '__main__':
    generate_json()
