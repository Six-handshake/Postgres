import postgres_service as ps

def generate_json():
    ps.connect_to_db()
    
if __name__ == '__main__':
    generate_json()