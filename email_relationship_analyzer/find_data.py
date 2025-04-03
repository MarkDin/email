
import json


def find_title_contain_reply(filepath: str, keyword: str, count: int):
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f) 
    cnt = 0
    for key, value in data.items():
        if 'count' in value and value['count'] == 1:
            for item in value['items']:
                if keyword.lower() in item[1].lower():
                    cnt += 1
                    print(json.dumps({
                        "email": item[0],
                        "subject": item[1],
                        "username": item[2],
                        "message_id": item[3]
                    }, ensure_ascii=False, indent=4))
        if cnt >= count:
            break
            

if __name__ == '__main__':
    find_title_contain_reply('relationships.json', '回复', 2)