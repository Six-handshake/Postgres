from postgres_srv import ssh_tests

def generate_json():
    print(ssh_tests.get_links_via_ssh('73', '76'))


if __name__ == '__main__':
    generate_json()
