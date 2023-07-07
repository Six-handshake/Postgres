from sshtunnel import SSHTunnelForwarder
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import text


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


def test_connection():
    with SSHTunnelForwarder(
            ('46.48.3.74', 22),  # Remote server IP and SSH port
            ssh_username="serv",
            ssh_password="12345678",
            remote_bind_address=('localhost', 5432)) as server:  # PostgreSQL server IP and sever port on remote machine

        server.start()  # start ssh sever
        print('Server connected via SSH')

        # connect to PostgreSQL
        local_port = str(server.local_bind_port)
        engine = create_engine('postgresql://postgres:postgres@127.0.0.1:' + local_port + '/postgres')
        Session = sessionmaker(bind=engine)
        session = Session()

        print('Database session created')

        sql = text('select * from "LinkedFaces" Where parent=179')

        # test data retrieval
        test = session.execute(sql)
        for t in test:
            print(t)
        session.close()

# Python implementation to print all paths from a source to destination

from collections import defaultdict

class Graph:

    def __init__(self, vertices):

        self.V = vertices
        self.graph = defaultdict(list)

    def addEdge(self, u, v):
        self.graph[u].append(v)

    def printAllPathsUtil(self, u, d, visited, path):

        visited[u] = True
        path.append(u)

        if u == d:
            print(path)
        else:
            for i in self.graph[u]:
                if visited[i] == False:
                    self.printAllPathsUtil(i, d, visited, path)
        path.pop()
        visited[u] = False

    def printAllPaths(self, source, destination):
        visited = [False]*(self.V)
        path = []
        self.printAllPathsUtil(source, destination, visited, path)


graph = Graph(5)
graph.addEdge(0, 1)
graph.addEdge(0, 4)
graph.addEdge(1, 2)
graph.addEdge(2, 1)
graph.addEdge(2, 3)
graph.addEdge(2, 4)
graph.addEdge(4, 2)
graph.addEdge(4, 3)

source = 0
destination = 3
print("All possible paths paths from % d to % d :" % (source, destination))

if __name__ == '__main__':
    #graph.printAllPaths(source, destination)
    test_connection()

