import numpy as np
import faust
import time
import torch
import cv2

from facenet_pytorch import MTCNN
from PIL import Image, ImageDraw


""" 
TODO autoscaling group for worker nodes
TODO figure out deployment
"""

device = torch.device('cuda:0' if torch.cuda.is_available() else 'cpu')
print('Running on device: {}'.format(device))
mtcnn = MTCNN(keep_all=True, device=device)

app = faust.App(
    'frames',
    broker='kafka://172.31.76.215:9092',
)

class Frame(faust.Record, serializer='json'):
    index: int
    frame: np.ndarray


frames_topic = app.topic('frames', value_type=Frame)


# @app.agent(agent_frame_sink)
# async def sink_it(stream):
#     async for frames in stream.take(50, within=10):
#         o_frames = sorted(frames, key = lambda i: i.index)
#         print([frame.index for frame in o_frames])


@app.agent(frames_topic)
async def frames(stream):
    async for frame_data in stream:

        #todo stream processing here
        print(frame_data.index)

        frame = np.asarray(frame_data.frame)

        boxes, _ = mtcnn.detect(frame)

        if (boxes is None):
            yield frame.resize((1920, 1080), Image.BILINEAR)

        frame_draw = frame.copy()
        draw = ImageDraw.Draw(frame_draw)

        for box in boxes:
            draw.rectangle(box.tolist(), outline=(255, 0, 0), width=6)

        # todo finish this
        yield frame_draw.resize((1920, 1080), Image.BILINEAR)
        #print(frames)
        # o_frames = sorted(frames, key = lambda i: i.index)
        # print([frame.index for frame in o_frames])
        # ix = frame['index']
        # kek = np.asarray(frame['frame'])
        # print(frame.index)
        # print(np.asarray(frame.frame).shape)
        #frame = np.frombuffer(frame, np.uint8)

        #todo write to s3/publish to s3 url that we show in flask 





