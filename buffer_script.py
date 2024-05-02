import json

from dejavu import Dejavu
from dejavu.logic.recognizer.file_recognizer import FileRecognizer
from dejavu.logic.recognizer.buffer_recognizer import BufferRecognizer
import os

# load config from a JSON file (or anything outputting a python dictionary)
with open("dejavu.cnf.SAMPLE") as f:
    config = json.load(f)

if __name__ == '__main__':

    # create a Dejavu instance
    djv = Dejavu(config)

    # list all files from aac folder
    folder_path = "aac"
    files = os.listdir(folder_path)
    aac_files = []
    for file in files:
        if file.endswith(".aac"):
            aac_files.append(file)
            break

    # Fingerprint all the aac's in the directory we give it
    for aac_file in aac_files:
        filepath = f"{folder_path}/{aac_file}"
        print(f"Fingerprinting {filepath}")
        # read binary file
        with open(filepath, "rb") as f:
            content = f.read()
            results = djv.recognize(BufferRecognizer, content, 48000, 1, 2) 
            print(f"Buffer Results: {results}")
    
        results = djv.recognize(FileRecognizer, filepath)
        print(f"File Results: {results}")
#ffprobe -v quiet -print_format json -show_format -show_streams -i pipe:0 < aac/1.aac
# (venv) ➜  dejavu git:(master) ✗ ffprobe -v quiet -print_format json -show_format -show_streams -i pipe:0 < aac/1-test.aac