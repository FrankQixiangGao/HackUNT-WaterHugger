def init_log(file_path):
    with open(file_path, 'w') as file:
        file.write('')
        file.close()
        
def log(msg, file_path, end='\n'):
    with open(file_path, 'a') as file:
        file.write(str(msg)+end)
        file.close()