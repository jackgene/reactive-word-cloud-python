import asyncio
from asyncio import Semaphore
from datetime import timedelta
from typing import Dict

import reactivex as rx
from reactivex import Observable
from reactivex import operators as ops
from reactivex.scheduler.eventloop import AsyncIOScheduler
from reactivex.scheduler.scheduler import Scheduler
from websockets.asyncio.server import ServerConnection, serve
from websockets.protocol import State

from .model import ChatMessage
from .service.word_cloud import make_chat_messages


async def start_server():
    port: int = 9673
    kafka_conf: Dict[str, str] = {
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': 'reactive-word-cloud-python',
        'enable.auto.commit': 'false',
        'auto.offset.reset': 'earliest'
    }
    chat_messages: Observable[ChatMessage] = make_chat_messages(
        kafka_conf=kafka_conf, topic_name='word-cloud.chat-message'
    )
    async def handle(ws_conn: ServerConnection):
        scheduler: Scheduler = AsyncIOScheduler(asyncio.get_event_loop())
        sem: Semaphore = Semaphore(0)
        def publish_chat_message(chat_message: ChatMessage) -> Observable[None]:
            print(f'Publishing: {chat_message}')
            return rx.from_future(asyncio.ensure_future(ws_conn.send(chat_message.to_json())))
        observable: rx.Observable[None] = chat_messages >> ops.take_while(
            lambda _: ws_conn.state == State.OPEN
        ) >> ops.throttle_first(
            timedelta(milliseconds=200)
        ) >> ops.concat_map(
            publish_chat_message
        )
        observable.subscribe(on_completed=lambda: sem.release(), scheduler=scheduler)
        await sem.acquire()
        print('Connection closed')

    async with serve(handle, 'localhost', port):
        print(f'server listening on port {port}')
        await asyncio.get_running_loop().create_future()  # run forever

def main(): asyncio.run(start_server())

if __name__ == '__main__': main()
