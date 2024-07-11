def get_file_path(dir_path:str, key_words:str):
    import os, re
    full_paths = []
    for dirpath, _, filenames in os.walk(dir_path):
        for filename in filenames:
            if re.match(key_words, filename):
                full_path = os.path.join(dirpath, filename)
                full_paths.append(full_path)
    
    return full_paths
