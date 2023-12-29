import time
from pprint import pprint

from flask import Flask, request, render_template, send_file
from threading import Thread
from pymongo import MongoClient

import subprocess
import twitch
import requests
import json
from datetime import datetime
from flask import redirect
import os
import glob
import shutil

from numba import jit

app = Flask(__name__)


@jit(parallel=True, forceobj=True)
def get_app_access_token(client_id, client_secret):
    payload = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials'
    }
    response = requests.post('https://id.twitch.tv/oauth2/token', params=payload)
    access_data = response.json()
    return access_data['access_token']


@jit(parallel=True, forceobj=True)
def is_channel_live(channel_name, client_id, oauth_token):
    headers = {
        'Client-ID': client_id,
        'Authorization': f'Bearer {oauth_token}',
    }
    response = requests.get(f'https://api.twitch.tv/helix/streams?user_login={channel_name}', headers=headers)
    channel_data = response.json()
    return channel_data['data'] and channel_data['data'][0]['type'] == 'live'


@jit(parallel=True, forceobj=True)
def record_stream(channel_name, output_file):
    command = f'streamlink https://www.twitch.tv/{channel_name} best -o {output_file} --force'
    subprocess.Popen(command, shell=True)


@jit(parallel=True, forceobj=True)
def convert_to_mp4(channel_name):
    timestamp = datetime.now().isoformat()
    input_file = f'temp/{channel_name}.ts'
    output_file = f'channels/{channel_name}/streams/{timestamp}.mp4'
    command = f'ffmpeg -i {input_file} -c:v copy -c:a copy {output_file}'
    subprocess.run(command, shell=True)

    # create channel folder
    if not os.path.exists(f'channels/{channel_name}/streams'):
        os.mkdir(f'channels/{channel_name}/streams')
    # move output file to channel folder
    shutil.move(f'temp/{channel_name}.txt', f'channels/{channel_name}/streams/{timestamp}.txt')


def record_chat(channel_name, output_file, oauth_token, nickname):
    subscription = twitch.Chat(channel=f'#{channel_name}', nickname=nickname,
                               oauth=oauth_token, helix=helix).subscribe(
        observer=lambda message: write_message_to_file(message, output_file))
    return subscription


def write_message_to_file(message, output_file):
    print(message.channel, message.sender, message.text)
    message_data = {
        'channel': message.channel,
        'sender': message.sender,
        'text': message.text,
        'timestamp': datetime.now().isoformat()
    }
    with open(output_file, 'a') as f:
        f.write(json.dumps(message_data) + '\n')


@jit(parallel=True, forceobj=True)
@app.route('/add_channel', methods=['POST'])
def add_channel():
    channel_name = request.form.get('channel_name')
    if channel_name and channel_name not in collection.find({}, {'name': 1}):
        value = {'name': channel_name}
        collection.insert_one(value)

        for user in helix.users([channel_name]):
            pprint(user.display_name)
            profile = user.profile_image_url
            # create channel folder
            if not os.path.exists(f'channels/{channel_name}'):
                os.mkdir(f'channels/{channel_name}')
            # download profile image
            with open(f'channels/{channel_name}/profile.png', 'wb') as handle:
                response = requests.get(profile, stream=True)
                if not response.ok:
                    print(response)
                for block in response.iter_content(1024):
                    if not block:
                        break
                    handle.write(block)
            # download offline image
            with open(f'channels/{channel_name}/offline.png', 'wb') as handle:
                response = requests.get(user.offline_image_url, stream=True)
                if not response.ok:
                    print(response)
                for block in response.iter_content(1024):
                    if not block:
                        break
                    handle.write(block)
        return redirect('/')
    else:
        return 'No channel name provided', 400


@jit(parallel=True, forceobj=True)
@app.route('/delete_channel', methods=['GET'])
def delete_channel():
    channel_name = request.args.get('channel_name')
    if channel_name and channel_name in collection.find_one({'name': channel_name})["name"]:
        collection.delete_one({'name': channel_name})

        live.remove(channel_name)
        convert_to_mp4(channel_name)
        recording.pop(channel_name).dispose()

        return redirect('/')
    else:
        return 'No channel name provided', 400


@jit(parallel=True, forceobj=True)
@app.route('/channels/<channel>/<image>', methods=['GET'])
def get_image(channel, image):
    with open(f'channels/{channel}/{image}', 'rb') as img:
        return send_file(f'channels/{channel}/{image}', mimetype='image/png')


@jit(parallel=True, forceobj=True)
@app.route('/channels/<channel>', methods=['GET'])
def get_streams(channel):
    folder_path = f'streams/{channel}/streams'
    files = glob.glob(f'{folder_path}/*.mp4')
    return render_template('channel.html', files=files, name=channel)


@jit(parallel=True, forceobj=True)
@app.route('/')
@app.route('/index')
def home():
    return render_template('index.html', channels=collection.find({}, {'name': 1}))


@jit(parallel=True, forceobj=True)
async def run_flask_app():
    Thread(target=app.run, kwargs={'port': 5000}).start()


@jit(parallel=True, forceobj=True)
def main(client_id, client_secret, oauth_token, nickname):
    run_flask_app()
    oauth = get_app_access_token(client_id, client_secret)

    while True:
        for channel_name in collection.find({}, {'name': 1}):
            is_live = is_channel_live(channel_name["name"], client_id, oauth)
            if is_live and channel_name["name"] not in live:
                print(f'{channel_name["name"]} has gone live!')
                live.append(channel_name["name"])

                record_stream(channel_name["name"], f'temp/{channel_name["name"]}.ts')
                recording[channel_name["name"]] = record_chat(channel_name["name"],
                                                              f'temp/{channel_name["name"]}.txt',
                                                              oauth_token, nickname=nickname)
            elif channel_name["name"] in recording and not is_live:
                print(f'{channel_name["name"]} is offline.')
                live.remove(channel_name["name"])

                convert_to_mp4(channel_name["name"])
                recording.pop(channel_name["name"]).dispose()
        time.sleep(60)


@jit(parallel=True, forceobj=True)
def get_collection(client, database_name, collection_name):
    db = client[database_name]
    coll = db[collection_name]
    return coll


def start():
    global collection
    global helix
    with open("config.json") as json_data_file:
        config = json.load(json_data_file)
        client_id = config["twitch"]["client_id"]
        client_secret = config["twitch"]["client_secret"]
        oauth_token = config["twitch"]["oauth_token"]
        nickname = config["twitch"]["nickname"]

        database_name = config["mongodb"]["database_name"]
        collection_name = config["mongodb"]["collection_name"]
        mongo_host = config["mongodb"]["mongo_host"]
        mongo_port = config["mongodb"]["mongo_port"]
        mongo_username = config["mongodb"]["mongo_username"]
        mongo_password = config["mongodb"]["mongo_password"]

        helix = twitch.Helix(client_id, client_secret)

        creds = f"{mongo_username}:{mongo_password}@" if mongo_username and mongo_password else ""
        client = MongoClient(f"mongodb://{creds}{mongo_host}:{mongo_port}/")
        collection = get_collection(client, database_name, collection_name)

        main(client_id, client_secret, oauth_token)


if __name__ == "__main__":
    collection = None
    helix = None
    live = []
    recording = {}

    start()
