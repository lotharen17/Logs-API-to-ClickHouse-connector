import clickhouse_connect
import sshtunnel

class ClickHouseConnector:
    """Class to create ClickHouse connection and (optionally) ssh tunnel. 
    Currently support only HTTP transmission. If you need to make it safer - use SSH connection. Otherwise, it's pretty safe
    to upload data through http on the same host. 
    
    Arguments:
        logger :instance of Logger object - logger to log all the operations inside. 
        login :str - login to ClickHouse server authorization. 
        password :str - password to ClickHouse server authorization. 
        host :str - IP or name of host with CH server. 
        port :int - TCP port number for CH server. 8123 for http by default and 8443 for https.
        db :str - name of database to connect. 


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

    FORMAT = "TSVWithNames" #Logs API format of response. 

    BadCode = 500
    SuccessCode = 200
    CloseCode = 205

    ChEndpoint = 'CH'
    SSHEndpoint = 'SSH'

    SSHBadDescription = "SSH connection not established. Check credentials and if ports are open."
    SSHSuccessDescription = "SSH connection successfully established."
    SSHCloseDescription = "SSH tunnel was successfully closed."

    ChBadDescription = "Connection to ClickHouse not established. Check credentials and ClickHouse server settings."
    ChSuccessDescription = "Connection to ClickHouse successfully established."
    ChCloseDescription = "Connection to Clickhouse was successfully closed."

    ChQueryBadDescription = "Query wasn't performed. Maybe user you logged with doesn't have permissions for that"
    ChQuerySuccessDescription = "Query was performed successfully"

    ChCreateTableBadDescription = "Table %s wasn't created. Maybe user you logged with doesn't have permission for that"
    ChCreateTableSuccessDescription = "Table %s was successfully created."

    ChInsertBadDescription = "Insert to ClickHouse wasn't performed. Maybe there are not enough rights, or bad/no data"
    ChInsertSuccessDescription = "Insert to ClickHouse was performed successfully."

    def __init__(self, logger, login, password, host, port, db, table, logTable=None, ssh=None):
        self.logger = logger
        self.login = login 
        self.password = password
        self.host = host
        self.port = port
        self.db = db 
        self.table = table 
        self.logTable = logTable
        self.ssh = ssh 
        if self.ssh is not None: 
            self.tunnel = self._establish_ssh_tunnel()
        self.ch_client = self._establish_ch_connection()
                
    def _establish_ssh_tunnel(self): 
        """Method to establish SSH tunnel, which will be then used for clickhouse connection.
    
        Arguments: self. All are taken from global ssh dictionary, which is a nested dictionary of ch_credentials.json config file. 

        Returns: tunnel object or None. 
        """
        sshtunnel.SSH_TIMEOUT = 10.0
        classmethod = f"Class: {self.__class__.__name__}. Method: {self._establish_ssh_tunnel.__name__}"

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
                    remote_bind_address = (remote_host_bind, remote_port_bind), 
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
    
    def _establish_ch_connection(self): 
        """Method to establish connection with ClickHouse server.
        
        Arguments: 
            self - uses global class' variables to establish connection
        """
        classmethod = f"Class: {self.__class__.__name__}. Method: {self._establish_ch_connection.__name__}"
        client = None
        
        if self.ssh is not None and self.tunnel is None: 
            print("ClickHouse connection cannot be establish, as SSH connection defined, but not established.")
            self.logger.add_to_log(response=ClickHouseConnector.BadCode, endpoint=ClickHouseConnector.ChEndpoint,
                                   description=ClickHouseConnector.ChBadDescription
                                   +ClickHouseConnector.SSHBadDescription).write_to_disk_incremental(classmethod)
        else: 
            try:
                client = clickhouse_connect.get_client(host=self.host, port=self.port, username=self.login, password=self.password
                                                       )
                print("ClickHouse connection established.")
                self.logger.add_to_log(response=ClickHouseConnector.SuccessCode, endpoint=ClickHouseConnector.ChEndpoint,
                                    description=ClickHouseConnector.ChSuccessDescription).write_to_disk_incremental(classmethod)
            except:
                print("Connection to ClickHouse not established.")
                self.logger.add_to_log(response=ClickHouseConnector.BadCode, endpoint=ClickHouseConnector.ChEndpoint,
                                   description=ClickHouseConnector.ChBadDescription).write_to_disk_incremental(classmethod)
        return client
    
    def re_establish_connection(self):  
        if self.ssh is not None: 
            self.tunnel = self._establish_ssh_tunnel()
        self.ch_client = self._establish_ch_connection()

    def close_connections(self):
        classmethod = f"Class: {self.__class__.__name__}. Method: {self.close_connections.__name__}"
        if self.ch_client is not None: 
            self.ch_client.close()
            print("ClickHouse connection closed.")
            self.logger.add_to_log(response=ClickHouseConnector.CloseCode, endpoint=ClickHouseConnector.ChEndpoint,
                                    description=ClickHouseConnector.ChCloseDescription).write_to_disk_incremental(classmethod)
        if self.tunnel is not None: 
            self.tunnel.close()
            print("SSH connection closed.")
            self.logger.add_to_log(response =ClickHouseConnector.CloseCode, endpoint=ClickHouseConnector.SSHEndpoint, 
                                       description = ClickHouseConnector.SSHCloseDescription).write_to_disk_incremental(classmethod)

    def query_data(self, query, **kwargs):
        classmethod = f"Class: {self.__class__.__name__}. Method: {self.query_data.__name__}"
        result = None
        try: 
            result = self.ch_client.query(query, **kwargs).result_rows
            self.logger.add_to_log(response=ClickHouseConnector.SuccessCode, endpoint=ClickHouseConnector.ChEndpoint,
                                    description=ClickHouseConnector.ChQuerySuccessDescription).write_to_disk_incremental(classmethod)
        except: 
            print("Query wasn't performed.")
            self.logger.add_to_log(response=ClickHouseConnector.BadCode, endpoint=ClickHouseConnector.ChEndpoint,
                                    description=ClickHouseConnector.ChQueryBadDescription).write_to_disk_incremental(classmethod)
        return result
    
    def create_table(self, query, table, **kwargs):
        classmethod = f"Class: {self.__class__.__name__}. Method: {self.create_table.__name__}"
        table_name = table
        result = False
        try: 
            self.ch_client.command(query, **kwargs).as_query_result()
            result = True
            self.logger.add_to_log(response=ClickHouseConnector.SuccessCode, endpoint=ClickHouseConnector.ChEndpoint,
                                    description=ClickHouseConnector.ChCreateTableSuccessDescription%table_name).write_to_disk_incremental(classmethod)
            print(f"Table {table_name} was successfully created.")
        except: 
            print(f"Table {table_name} wasn't created")
            self.logger.add_to_log(response=ClickHouseConnector.BadCode, endpoint=ClickHouseConnector.ChEndpoint,
                                    description=ClickHouseConnector.ChCreateTableBadDescription%table_name).write_to_disk_incremental(classmethod)
        return result
    
    def insert_datafile(self, file, settings=None): 
        result = False
        try: 
            clickhouse_connect.driver.tools.insert_file(self.ch_client,self.table, file, settings=settings, database = self.db, fmt = ClickHouseConnector.FORMAT)
            result = True
        finally: 
            return result

    def insert_data(self, table, data): 
        result = False 
        try: 
            self.ch_client.insert(table, data, database=self.db)
            result = True
        finally: 
            return result 
        
    