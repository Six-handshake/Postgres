import src.postgres_srv as ps


def generate_json():
    print(ps.get_links_via_ssh('73', '76'))


if __name__ == '__main__':
    generate_json()
