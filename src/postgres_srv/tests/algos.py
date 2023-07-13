from sshtunnel import SSHTunnelForwarder
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import text
import pprint
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
            remote_bind_address=('localhost', SSH_PORT)) as server:  # PostgreSQL server IP and sever port on remote machine

        server.start()  # start ssh sever
        print('Server connected via SSH')

        # connect to PostgreSQL
        local_port = str(server.local_bind_port)
        engine = create_engine('postgresql://postgres:postgres@127.0.0.1:' + local_port + '/postgres')
        Session = sessionmaker(bind=engine)
        session = Session()

        print('Database session created')
        graph = BFSGraph(session)
        depth = 0
        pprint.pprint(graph.find_children(73,991))
        pprint.pprint(graph.graph)



        session.close()

class RelationsGraph:
    graph = {

    }

    depth = 0

    def __init__(self,session):
        self.session = session

    def construct_entity(self, le, query_result):
        entity = {
            'legal_entity': le,
            'parents': [],
            'children': []
        }
        for r in query_result:
            if r[1] != le:
                entity['parents'].append(r[1])
            else:
                entity['children'].append(r[2])

        return entity

    def make_starting_entity(self, le):
        query_result = self.execute_query(le)
        starting_point = self.construct_entity(le, query_result)
        return starting_point

    def execute_query(self, le):
        sql = text(f'select * from "LinkedFaces" Where parent={le} or child={le}')
        query_result = self.session.execute(sql)
        return query_result

class RecursiveGraph:
    graph = {
        1: [],
        2: [],
        3: [],
        4: [],
        5: [],
        6: []
    }


    def __init__(self, session):
        self.session = session

    def construct_entity(self, le, query_result):
        entity = {
            'legal_entity': le,
            'children': []
        }
        for r in query_result:
            entity['children'].append(r[2])

        return entity


    def make_starting_entity(self, le):
        query_result = self.execute_query(le)
        starting_point = self.construct_entity(le, query_result)
        return starting_point

    def execute_query(self, le):
        sql = text(f'select * from "LinkedFaces" Where parent={le}')
        query_result = self.session.execute(sql)
        return query_result

    def find_children(self, le, depth):
        queue = []
        depth += 1
        queue.append(self.make_starting_entity(le))
        while queue:
            current_entity = queue.pop()
            self.graph[depth].append(current_entity)
            if current_entity['children'] is not None and depth < 6:
                for child in current_entity['children']:
                    self.find_children(child, depth)


        return self.graph

class BFSGraph:
    graph = {
        1: [],
        2: [],
        3: [],
        4: [],
        5: [],
        6: []
    }

    def __init__(self, session):
        self.session = session


    def construct_entity(self, le, query_result):
        entity = {
            'legal_entity': le,
            'children': []
        }
        for r in query_result:
            entity['children'].append(r[2])

        return entity

    def construct_entity_start(self, le, query_result):
        entity = {
            'legal_entity': le,
            'children': []
        }
        for r in query_result:
            entity['children'].append(r[1])

        return entity

    def execute_query(self, le):
        sql = text(f'select * from "LinkedFaces" Where parent={le}')
        query_result = self.session.execute(sql)
        return query_result

    def make_starting_entity(self, le):
        sql = text(f'select * from "LinkedFaces" Where child={le}')
        query_result = self.session.execute(sql)
        start = self.construct_entity_start(le, query_result)
        return start

    def find_children(self,le1,le2):
        queue = []

        #start = self.make_starting_entity(le)['children']
        #print(start)
        queue.append([le1])

        while queue:
            children = queue.pop(0)
            #print(children)
            node = children[-1]
            #print(node)
            self.graph[len(children)].append(node)
            pprint.pprint(self.graph)

            if len(children) == 6 or node==le2:
                print(children)
                break

            query = self.execute_query(node)
            new_children = self.construct_entity(node, query)['children']

            for child in new_children:
                new_path = list(children)
                new_path.append(child)
                queue.append(new_path)


if __name__ == '__main__':
    test_connection()

