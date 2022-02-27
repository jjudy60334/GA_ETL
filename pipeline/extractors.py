import json


class JsonExtractor:
    # def __init__(self):
    def extract(self, file_path: str) -> list:
        with open(file_path) as f:
            lines = f.readlines()
        f.close()
        datas = []
        for i in lines:
            datas.append(json.loads(i))
        return datas
