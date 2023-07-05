from sshtunnel import SSHTunnelForwarder
from sqlalchemy.orm import sessionmaker 
from sqlalchemy import create_engine
from sqlalchemy import text

with SSHTunnelForwarder(
    ('46.48.3.74', 22), #Remote server IP and SSH port
    ssh_username = "serv",
    ssh_password = "12345678",
    remote_bind_address=('localhost', 5432)) as server: #PostgreSQL server IP and sever port on remote machine
        
    server.start() #start ssh sever
    print ('Server connected via SSH')
    
    #connect to PostgreSQL
    local_port = str(server.local_bind_port)
    engine = create_engine('postgresql://postgres:postgres@127.0.0.1:' + local_port +'/postgres')

    Session = sessionmaker(bind=engine)
    session = Session()
    
    print ('Database session created')
    
    sql = text('select version()')
   
    #test data retrieval
    test = session.execute(sql)
    for t in test:
        print(t)

    session.close()