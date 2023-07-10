from sshtunnel import SSHTunnelForwarder
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import text
import pprint

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
            ('46.48.3.74', 22),  # Remote server IP and SSH port
            ssh_username="serv",
            ssh_password="12345678",
            remote_bind_address=('localhost', 5332)) as server:  # PostgreSQL server IP and sever port on remote machine

        server.start()  # start ssh sever
        print('Server connected via SSH')

        # connect to PostgreSQL
        local_port = str(server.local_bind_port)
        engine = create_engine('postgresql://postgres:postgres@127.0.0.1:' + local_port + '/postgres')
        Session = sessionmaker(bind=engine)
        session = Session()

        print('Database session created')
        graph = NeighboursGraph(session)
        depth = 0
        pprint.pprint(graph.find_children(73,depth))




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

class NeighboursGraph:
    graph = {
        0: [],
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
            print(queue)
            current_entity = queue.pop()
            self.graph[depth].append(current_entity)
            if current_entity['children'] is not None and depth < 6:
                for child in current_entity['children']:
                    self.find_children(child, depth)
            else:
                print(current_entity)


        return self.graph

if __name__ == '__main__':
    test_connection()

