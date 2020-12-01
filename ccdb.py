import faust
import numpy as np


app = faust.App(
    'goodbye-world',
    broker='kafka://172.31.76.215:9092',
    topic_partitions=8,

)

class Frame(faust.Record, serializer='json'):
    index: int
    frame: np.ndarray


print(Frame(index=0, frame=np.ones(1)).dumps(serializer='pickle'))

images_topic = app.topic('slags-2', value_type=Frame)


@app.agent(images_topic)
async def images(stream):
    async for frames in stream.take(30, within=5):
        #print(frames)
        o_frames = sorted(frames, key = lambda i: i.index)
        print([frame.index for frame in o_frames])
        # ix = frame['index']
        # kek = np.asarray(frame['frame'])
        # print(frame.index)
        # print(np.asarray(frame.frame).shape)
        #frame = np.frombuffer(frame, np.uint8)





