from sshtunnel import SSHTunnelForwarder
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import text
import os
from dotenv import load_dotenv

load_dotenv()
HOST = os.getenv('HOST')
IP_ADDRESS = os.getenv('IP_ADDRESS')
SSH_PORT = int(os.getenv('PORT'))
SSH_USERNAME = os.getenv('SSH_USERNAME')
SSH_PASSWORD = os.getenv('SSH_PASSWORD')
TABLE_NAME = os.getenv('TABLE_NAME')

graph1 = {
        '73': ['188', '185'],
        '188': ['220', '191', '103', '11'],
        '185': ['95', '104', '196'],
        '95': ['73', '146', '119', '124', '137'],
        '104': ['55', '127','216','160'],
        '196': ['298', '199', '76', '172', '186', '124', '74'],
        '128': ['38', '147', '76', '88'],
        '294': ['76', '127']
        }
draft_graph = [
        {
            'legal_entity': 73,
            'parents': [133, 282, 64, 38, 300, 95, 16],
            'children': [188, 185]
        },
        {
            'legal_entity': 73,
            'parents': [133, 282, 64, 38, 300, 95, 16],
            'children': [188, 185]
        }

]

def test_connection():
    with SSHTunnelForwarder(
            (IP_ADDRESS, 22),  # Remote server IP and SSH port
            ssh_username=SSH_USERNAME,
            ssh_password=SSH_PASSWORD,
            remote_bind_address=(IP_ADDRESS, SSH_PORT)) as server:  # PostgreSQL server IP and sever port on remote machine

        server.start()  # start ssh sever
        print('Server connected via SSH')

        # connect to PostgreSQL
        local_port = str(server.local_bind_port)
        engine = create_engine('postgresql://postgres:postgres@127.0.0.1:' + local_port + '/postgres')
        Session = sessionmaker(bind=engine)
        session = Session()

        print('Database session created')
        sql = text(f'select * from "LinkedFaces" Where parent=17')
        query_result = session.execute(sql)
        for q in query_result:
            print(q)

        session.close()



if __name__ == '__main__':
    test_connection()

