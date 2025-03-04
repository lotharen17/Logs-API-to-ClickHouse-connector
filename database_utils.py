from logger import Logger
import clickhouse_connect
import sshtunnel


class ClickHouseConnector:
    """Class to create SSHTunnel to remote host with ClickHouse installed
    
    Arguments:
        remote_host :str - remote host name/ip address. 
        remote_tcp_port :int - TCP port of remote host. 
        username :str - username to access remote host. 
        password :str - password of remote host's users. 
        remote_host_bind :str - host to bid remotely. Usually, localhost. 
        remote_tcp_port_bind :int - remote TCP port to bind.
        local_host_bind  :str - local host to bind, usually, localhost. 
        local_tcp_port_bind - local TCP port to bind with remote host. 

    Methods: 
        create_tunnel(self) - to establish connection with class' init arguments. Called in init by default. 
        tunnel_start(self) - to start the tunnel connection.
        tunnel_stop(self) - to stop the tunnel connection.
        

    Returns: 
        tunnel :instance of SSHtunnel class or None - SSH tunnel to connect. 
    """


    BadCode = 504
    SuccessCode = 200

    ChEndpoint = 'CH'
    SSHEndpoint = 'SSH'

    SSHBadDescription = "SSH connection not established. Check credentials and if ports are open."
    SSHSuccessDescription = "SSH connection successfully established."

    ChBadDescription = "Connection to ClickHouse not established. Check credentials and ClickHouse server settings."
    ChSuccessDescription = "Connection to ClickHouse successfully established."

    def __init__(self, logger, login, password, host, port, db, table, logTable=None, interface='http', ssh=None):
        self.logger = logger
        self.login = login 
        self.password = password
        self.host = host
        self.port = port
        self.db = db 
        self.table = table 
        self.logTable = logTable
        self.interface = interface
        self.ssh = ssh 
        if self.ssh is not None: 
            self.tunnel = self.establish_ssh_tunnel()
        self.ch_client = self.establish_clickhouse_connection()
                
    def establish_ssh_tunnel(self): 
        """Method to establish SSH tunnel, which will be then used for clickhouse connection.
    
        Arguments: self. All are taken from global ssh dictionary, which is a nested dictionary of ch_credentials.json config file. 

        Returns: tunnel object or None. 
        """
        sshtunnel.SSH_TIMEOUT = 10.0
        classmethod = f"Class: {self.__class__.__name__}. Method: {self.establish_ssh_tunnel.__name__}"

        tunnel = None

        if self.ssh is not None: 
            host = self.ssh.get('host')
            port = self.ssh.get('port') if self.ssh.get('port') is not None else 22
            login = self.ssh.get('login')
            password = self.ssh.get('password') if self.ssh.get('password') is not None else ''
            remote_host_bind = 'localhost'
            remote_port_bind = self.ssh.get('remote_port_bind')
            local_host_bind = 'localhost'
            local_port_bind = self.port

            def tunneling_with_ports_autoassignation():
                tunnel = sshtunnel.SSHTunnelForwarder(
                    host, port, ssh_username = login, ssh_password = password, 
                    remote_bind_address = (remote_host_bind, remote_port_bind), 
                    host_pkey_directories=[]
                )
                return tunnel
            
            if self.port is None: 
                try: 
                    tunnel = tunneling_with_ports_autoassignation()
                    tunnel.start()
                    self.port = tunnel.local_bind_port

                except:
                    tunnel = None
                    print('SSH connection cannot be established')

            else:
                try: 
                    tunnel = sshtunnel.SSHTunnelForwarder(
                    host, port, ssh_username = login, ssh_password = password, 
                    remote_bind_address = (host, remote_port_bind), 
                    local_bind_address = (local_host_bind, local_port_bind),
                    host_pkey_directories=[]
                    )
                    tunnel.start()
                except:
                    try: 
                        tunnel = tunneling_with_ports_autoassignation()
                        tunnel.start()
                        self.port = tunnel.local_bind_port
                        print(f"Due to error (port was busy) local binded port was re-assigned. The new port is {self.port}")
                    except:
                        tunnel = None
                        print('SSH connection cannot be established')

            
            if tunnel is None: 
                print("SSH connection not established. See examples of SSH sub-dictionary on: https://github.com/lotharen17/Metrica-OOP-extractor?tab=readme-ov-file#config-files ")

                self.logger.add_to_log(response=ClickHouseConnector.BadCode, endpoint=ClickHouseConnector.SSHEndpoint, 
                                       description= ClickHouseConnector.SSHBadDescription).write_to_disk_incremental(classmethod)
            else: 
                print("SSH connection successfully established")
                self.logger.add_to_log(response =ClickHouseConnector.SuccessCode, endpoint=ClickHouseConnector.SSHEndpoint, 
                                       description = ClickHouseConnector.SSHSuccessDescription).write_to_disk_incremental(classmethod)
                
        return tunnel
    
    def establish_clickhouse_connection(self): 
        classmethod = f"Class: {self.__class__.__name__}. Method: {self.establish_ssh_tunnel.__name__}"
        client = None
        
        if self.ssh is not None and self.tunnel is None: 
            print("ClickHouse connection cannot be establish, as SSH connection defined, but not established.")
            self.logger.add_to_log(response=ClickHouseConnector.BadCode, endpoint=ClickHouseConnector.ChEndpoint,
                                   description=ClickHouseConnector.ChBadDescription
                                   +ClickHouseConnector.SSHBadDescription).write_to_disk_incremental(classmethod)
        else: 
            client = clickhouse_connect.get_client(host=self.host, port=self.port, username=self.login, password=self.password, interface=self.interface)
            print("ClickHouse connection established.")
            self.logger.add_to_log(response=ClickHouseConnector.SuccessCode, endpoint=ClickHouseConnector.ChEndpoint,
                                   description=ClickHouseConnector.ChSuccessDescription).write_to_disk_incremental(classmethod)
            
        return client
        


    


        # self.tunnel.start()






# server = SSHTunnelForwarder(
#     'alfa.8iq.dev',
#     ssh_username="pahaz",
#     ssh_password="secret",
#     remote_bind_address=('127.0.0.1', 8080)
# )

# server.start()

# print(server.local_bind_port)  # show assigned local port
# # work with `SECRET SERVICE` through `server.local_bind_port`.

# server.stop()

# def clickhouse_connector(host='localhost', tcp_port = 666, username = 'default', password='default'):
#     """Function to establish a connection with clickhouse server."""
#     #SSH connection creation 
#     def ssh_connection_creation(host, tcp_port):
#         """Function, that reads global varibale ssh_path and creates. Needs ssh_path global variable to run proprely."""
#         if 'ssh_path' in globals(): 
#             try:
#                 with open(ssh_path, "r") as s:
#                     ssh_json = s.read()
#                     ssh_json = json.loads(ssh_json)
#                 server = sshtunnel.SSHTunnelForwarder(
#                     (ssh_json['host'], ssh_json['port']),
#                     ssh_username = ssh_json['login'],
#                     ssh_password = ssh_json['password'],
#                     #ssh_private_key = "id_ed25519",
#                     #ssh_host_key = 'vanoing',
#                     remote_bind_address = ('localhost', ssh_json['remote_port_bind']),
#                     ssh_private_key_password = ssh_json['password'],
#                     local_bind_address=(host, tcp_port), 
#                     host_pkey_directories=[], 
#                     set_keepalive=2.
#                     )
#                 server.start()
#                 return server
#             except: 
#                 logger(response ='\t404', endpoint=f'\t{ssh_path}', description = '\tFile with ssh config not fount\n', path=log_path)
#                 return None
#         else: 
#             return None 
#     ssh_connection = ssh_connection_creation(host, tcp_port)
# #     ssh_connection.stop()
# #    ssh_connection.start() 
#     try: 
#         client = clickhouse_connect.get_client(host=host, port=tcp_port, username=username, password=password)
#         logger(response ='\t200', endpoint=f'\t{host}:{tcp_port}', description = '\tSuccessfull connection to ClickHouse\n', path=log_path)
#     except: 
#         re_run=1 
#         logger(response ='\t404', endpoint=f'\t{host}:{tcp_port}', description = '\tConnection to clickhouse failed miserably.\n', path=log_path)
#         client = None 
#     return client




    # def establish_connection(self): 
    #     """Method to initialize tunnel"""
    #     try: 
    #         tunnel = sshtunnel.SSHTunnelForwarder(
    #             self.remote_host, 
    #             self.remote_tcp_port, 
    #             ssh_username = self.username,
    #             ssh_password = self.password, 
    #             remote_bind_address = (self.remote_host_bind, self.remote_tcp_port_bind), 
    #             local_bind_address = (self.local_host_bind, self.local_tcp_port_bind),
    #             host_pkey_directories=[]
    #         )
    #     except BaseSSHTunnelForwarderError:
    #         print()
    #     return tunnel