import os 
import json


class DatabaseException(Exception): 
    pass

class FlowException(Exception): 
    pass


class UtilsSet:
# Files reads/writes to handle with  except(OSError, IOError) in wrappers.       print(f"You probably don't have an acess to {path} file.")
    """Class to speed up usage of routine operations of reading/writing of files in project"""

    def __init__(self): 
        return None
    
    def create_folder(self, dirpath):
        """Method to create folder/directory."""
        os.makedirs(dirpath, exist_ok=True)
        print(f"Directory {dirpath} created")
        return self
    
    def write_to_file(self, content, path): 
        """Method that writes (appends) content to file. Creates both file and directory (folder) if needed.
                
        Arguments: 
            content :str(text) - content of file. 
            path :str - path to file. 
        """

        def nested_writer(): 
            "Nested function to append data to file"
            with open(path, "a", encoding="utf-8", newline='\n') as f:
                f.write(content)

        if os.path.exists(path): 
            nested_writer()

        else: 
            if len(path.split('/')) > 1: 
                dirpath = '/'.join(path.split('/')[:-1:])+'/'
                filename = path.split('/')[-1]
                if not os.path.isdir(dirpath): 
                    try: 
                        self.create_folder(dirpath)
                    except(OSError, IOError): 
                        print(f"Directory {dirpath} wasn't created. Probably, you don't have proper permission. Trying to write file to the main directory." )
                        path = filename
            nested_writer()
        return self

    def rewrite_file(self, content, path): 
        """Function that rewrites file. Creates both file and directory (folder) if needed.
        
        Arguments: 
            content :str(text) - content of file. 
            path :str - path to file. 
        """

        def nested_writer(): 
            """Function that rewrites file. Creates both file and directory (folder) if needed."""
            with open(path, "w", encoding="utf-8", newline='\n') as f:
                f.write(content)

        if os.path.exists(path): 
            nested_writer()

        else: 
            if len(path.split('/')) > 1: 
                dirpath = '/'.join(path.split('/')[:-1:])+'/'
                filename = path.split('/')[-1]
                if not os.path.isdir(dirpath): 
                    try: 
                        self.create_folder(dirpath)
                    except(OSError, IOError): 
                        print(f"Directory {dirpath} wasn't created. Probably, you don't have proper permission. Trying to write file to the main directory." )
                        path = filename
            nested_writer()
        return self 
    
    def delete_file(self, path):
        if os.path.exists(path):
            os.remove(path)
        else: 
            print(f"File {path} doesn't exist.")
        return self 
    
    def read_file(self, path): 
        with open(path, "r") as f: 
            content = f.read()
        return content
    
    def read_sql_file(self, path):
        content = self.read_file(path)
        content = " ".join(line.strip() for line in content.splitlines())
        content = content.strip()
        return content
    
    def read_json_file(self, path): 
        content = self.read_file(path)
        content = json.loads(content)
        return content