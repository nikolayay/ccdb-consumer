import numpy as np
import faust
import time
import torch
import cv2
import json
import boto3
import sys
from datetime import datetime
import uuid

from facenet_pytorch import MTCNN
from PIL import Image, ImageDraw
from joblib import Parallel, delayed



""" 
TODO autoscaling group for worker nodes
TODO figure out deployment
"""

device = torch.device('cuda:0' if torch.cuda.is_available() else 'cpu')
mtcnn = MTCNN(keep_all=True, device=device)

s3 = boto3.client('s3')

# check we have creds
try:
    print([obj for obj in boto3.resource('s3').Bucket('ccdb-cw-bucket').objects.all()])
except:
    print("ERROR: creds missing")
    sys.exit(1)

app = faust.App(
    'frames',
    broker='kafka://172.31.76.215:9092',
    topic_partitions=8
)


class Frame(faust.Record, serializer='json'):
    index: int
    filename: str
    width: int
    height: int
    frame: np.ndarray

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


frames_topic = app.topic('frames-2', value_type=Frame)

# last_frames = app.Table('last_frames', value_type=Frame,
#                         help='Keep track of the last frame to show')

# @app.agent(agent_frame_sink)
# async def sink_it(stream):
#     async for frames in stream.take(50, within=10):
#         o_frames = sorted(frames, key = lambda i: i.index)
#         print([frame.index for frame in o_frames])


def proc_frame_data(frame_data):

    # preprocessing
    frame = np.asarray(frame_data.frame, dtype=np.uint8)
    
    # uncompressing the jpg
    frame = cv2.imdecode(frame,cv2.IMREAD_GRAYSCALE)
    frame = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))

    boxes, _ = mtcnn.detect(frame)

    if (boxes is None):
        return frame.resize((frame_data.width, frame_data.height), Image.BILINEAR)

    frame_draw = frame.copy()
    draw = ImageDraw.Draw(frame_draw)

    for box in boxes:
        draw.rectangle(box.tolist(), outline=(255, 0, 0), width=6)


    #frame_draw.resize((frame_data.width, frame_data.height), Image.BILINEAR)
    print(np.array(frame_draw).shape)
    print(frame_data.index)

    # todo move that to separate agent
    # store last frame ! probably as jpg...
    # o = {
    #     'index': -1,
    #     'filename': frame_data.filename,
    #     'frame': json.dumps(np.array(frame_draw), cls=NumpyEncoder).encode('utf-8') # ! converting to numpy
    #     }

    # last_frames[frame_data.filename] = o

def s3_write(frame_data):
    print(f'AGENT YIELD: {frame_data.filename!r}')

    ret, encoded_frame = cv2.imencode('.jpg',frame_data.frame)
    now = datetime.now()
    key = f'frames/{frame_data.filename}/{now.year}/{now.month}/{now.day}/{now.hour}/{uuid.uuid1()}.jpg'


    s3.put_object(Body=encoded_frame.tobytes(), 
                  Bucket='ccdb-cw-bucket', 
                  Key=key)



@app.agent(frames_topic, concurrency=4, sink=[s3_write])
async def frames(stream):
    async for frame_data in stream:

         # preprocessing
        frame = np.asarray(frame_data.frame, dtype=np.uint8)
        
        # uncompressing the jpg
        frame = cv2.imdecode(frame,cv2.IMREAD_UNCHANGED)
        frame = Image.fromarray(frame)

        boxes, _ = mtcnn.detect(frame)

        if (boxes is None):
            print('no faces')
            yield Frame(frame_data.index, 
                    frame_data.filename, 
                    frame_data.width, 
                    frame_data.height, 
                    frame=np.array(frame_draw))

        print(len(boxes))


        frame_draw = frame.copy()
        draw = ImageDraw.Draw(frame_draw)

        for box in boxes:
            draw.rectangle(box.tolist(), outline=(255, 0, 0), width=6)

        #frame_draw = frame_draw.resize((frame_data.width, frame_data.height), Image.BILINEAR)

        yield Frame(frame_data.index, 
                    frame_data.filename, 
                    frame_data.width, 
                    frame_data.height, 
                    np.array(frame_draw))
        

# @app.page('/{filename}')
# @app.table_route(table=last_frames, match_info='filename')
# async def get_count(web, request, filename):
   
#     return web.json({
#         'last_frame': last_frames[filename],
#     })