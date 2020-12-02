import faust
import numpy as np
import time

""" 
TODO change names to not be explicit
TODO scale up boxes to run pytorch + GPUs yeehaw
     run two workers
TODO autoscaling group for worker nodes
TODO figure out deployment
"""

app = faust.App(
    'cum-world',
    broker='kafka://172.31.76.215:9092',
)

class Frame(faust.Record, serializer='json'):
    index: int
    frame: np.ndarray


images_topic = app.topic('slags-3', value_type=Frame)


# @app.agent(agent_frame_sink)
# async def sink_it(stream):
#     async for frames in stream.take(50, within=10):
#         o_frames = sorted(frames, key = lambda i: i.index)
#         print([frame.index for frame in o_frames])


@app.agent(images_topic, concurrency=3)
async def images(stream):
    async for frame in stream:

        #todo stream processing here
        print(frame.index)
        #print(frames)
        # o_frames = sorted(frames, key = lambda i: i.index)
        # print([frame.index for frame in o_frames])
        # ix = frame['index']
        # kek = np.asarray(frame['frame'])
        # print(frame.index)
        # print(np.asarray(frame.frame).shape)
        #frame = np.frombuffer(frame, np.uint8)

        #todo write to s3/publish to s3 url that we show in flask 





