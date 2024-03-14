import requests
import sys


if __name__ == "__main__":

    if len(sys.argv) <= 1:
        print("You need ti provide action")
        exit(0)

    url = 'https://d5dmu46v9e4bggfs7n4a.apigw.yandexcloud.net/?'
    url += "action=" + sys.argv[1]
    if len(sys.argv) == 3:
         url += "&task_id=" + sys.argv[2]

    if sys.argv[1] == "convert":
        response = requests.get(url)
        if response.headers['content-type'] == 'application/json':
            data = response.json()
            print("Your task_id: ", data['task_id'])
            print("Your credentials: ", data['presigned_url'])
            print("Now we are going to send the image")


        url = data['presigned_url']['url']
        payload = data['presigned_url']['fields']
        headers = {}

        files=[
            ('file',('lenna.jpg',open('lenna.jpg','rb'),'image/jpeg'))
        ]
        response = requests.post(url, headers=headers, files=files, data=payload)
        print(response.status_code)

    else:
        response = requests.get(url)
        if response.headers['content-type'] == 'application/json':
            data = response.json()
            print(data)



