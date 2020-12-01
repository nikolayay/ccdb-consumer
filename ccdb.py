import faust
import numpy as np

app = faust.App(
    'hello-world',
    broker='kafka://172.31.76.215:9092',
)


greetings_topic = app.topic('stringies', value_serializer='raw')

images_topic = app.topic('images', value_serializer='raw')


@app.agent(images_topic)
async def images(stream):
    async for k, frame in stream.items():
        
        print('bonk')
        print(k)
        frame = np.frombuffer(frame, np.uint8)


@app.agent(greetings_topic)
async def greet(greetings):
    async for greeting in greetings:
        print(greeting)





