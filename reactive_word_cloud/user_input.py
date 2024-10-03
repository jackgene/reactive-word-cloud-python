import asyncio
import logging
from asyncio import Task
from typing import Literal

import reactivex as rx
import reactivex.operators as ops
import websockets.asyncio.client as ws_client
from aiokafka import AIOKafkaConsumer, ConsumerRecord
from reactivex import Observable
from reactivex.abc import DisposableBase, ObserverBase, SchedulerBase
from reactivex.disposable import Disposable
from websockets.exceptions import ConnectionClosed
from websockets.protocol import State
from websockets.asyncio.connection import Connection

from reactive_word_cloud.config import KafkaConfig, WebSocketsConfig
from reactive_word_cloud.model import SenderAndText


def from_kafka(config: KafkaConfig) -> Observable[SenderAndText]:
    def consume_messages(observer: ObserverBase[bytes], _: SchedulerBase | None) -> DisposableBase:
        consumer: AIOKafkaConsumer = AIOKafkaConsumer(
            *config.topic_names,
            bootstrap_servers=config.bootstrap_servers,
            group_id=config.group_id,
            enable_auto_commit=config.enable_auto_commit,
            auto_offset_reset=config.auto_offset_reset
        )
        async def consume():
            await consumer.start()
            try:
                msg: ConsumerRecord[bytes, bytes]
                async for msg in consumer:
                    if msg.value is not None:
                        observer.on_next(msg.value)
            except Exception as e:
                observer.on_error(e)
            finally:
                await consumer.stop()
                observer.on_completed()
        asyncio.create_task(consume())

        def dispose():
            asyncio.run_coroutine_threadsafe(consumer.stop(), asyncio.get_running_loop())

        return Disposable(dispose)

    def extract_chat_message(msg_value: bytes) -> Observable[SenderAndText]:
        sender_text: SenderAndText | None = SenderAndText.from_json(msg_value.decode('utf-8'))
        if sender_text is not None:
            logging.info(f'consumed: {sender_text.to_json()}')
        return rx.empty() if sender_text is None else rx.just(sender_text)

    return rx.create(consume_messages) >> ops.concat_map(extract_chat_message)


def from_websockets(config: WebSocketsConfig) -> Observable[SenderAndText]:
    def consume_messages(observer: ObserverBase[str], _: SchedulerBase | None) -> DisposableBase:
        done: asyncio.Semaphore = asyncio.Semaphore(0)
        ws_conn: ws_client.connect = ws_client.connect(config.url)
        async def consume():
            try:
                conn: Connection
                async with ws_conn as conn:
                    await_done_task: Task[Literal[True]] = asyncio.create_task(done.acquire())
                    while True:
                        msgs: set[Task[str | bytes] | Task[Literal[True]]]
                        pendings: set[Task[str | bytes] | Task[Literal[True]]]
                        msgs, pendings = await asyncio.wait(
                            [asyncio.create_task(conn.recv()), await_done_task],
                            return_when=asyncio.FIRST_COMPLETED
                        )
                        msg: str | bytes | bool = msgs.pop().result()
                        if isinstance(msg, str):
                            observer.on_next(msg)
                        elif msg == True:
                            observer.on_completed()
                            for pending in pendings:
                                pending.cancel()
                            break
            except Exception as e:
                logging.error(f'error while receiving websocket messages: {e}')
                observer.on_error(e)
        consumer_task = asyncio.create_task(consume())

        def dispose():
            done.release()
            def await_connection_close():
                from time import sleep
                while not (consumer_task.done() and ws_conn.connection.state == State.CLOSED):
                    sleep(0.05)
                logging.info('user input consumer WebSockets connection closed')
            # Python seems to wait for this to complete before shutting down
            asyncio.run_coroutine_threadsafe(asyncio.to_thread(await_connection_close), asyncio.get_running_loop())

        return Disposable(dispose)

    def extract_chat_message(json: str) -> Observable[SenderAndText]:
        sender_text: SenderAndText | None = SenderAndText.from_json(json)
        if sender_text is not None:
            logging.info(f'consumed: {sender_text.to_json()}')
        return rx.empty() if sender_text is None else rx.just(sender_text)

    def handle_closure(err: Exception, obs: Observable[SenderAndText]) -> Observable[SenderAndText]:
        return from_websockets(config) >> ops.delay(1) if isinstance(err, ConnectionClosed) else obs

    return rx.create(consume_messages) \
        >> ops.concat_map(extract_chat_message) \
        >> ops.catch(handle_closure)
