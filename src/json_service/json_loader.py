import postgres_service as ps
import ssh_tests

def generate_json():
    ssh_tests.connect_to_db()
    
if __name__ == '__main__':
    generate_json()