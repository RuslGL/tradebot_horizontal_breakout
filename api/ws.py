import asyncio
import aiohttp
import json
import traceback


class SocketBybit:

    def __init__(self, url, params=None, on_message=None):
        self.url = url
        self.params = params
        if on_message is not None:
            self.on_message = on_message

    async def connect(self):
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.url) as ws:
                        await self.on_open(ws)
                        heartbeat_task = asyncio.create_task(self.send_heartbeat(ws))
                        while True:
                            try:
                                message = await ws.receive()
                                if message.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                    raise ConnectionError("WebSocket connection closed or error occurred")
                                elif message.type == aiohttp.WSMsgType.TEXT:
                                    await self.on_message(ws, message)
                            except Exception as e:
                                await self.on_error(ws, e)
                                await asyncio.sleep(5)
                                break  # Exit the inner loop to reconnect
            except Exception as e:
                print(f"Connection failed: {e}")
                await asyncio.sleep(1)  # Wait before attempting to reconnect

    async def send_heartbeat(self, ws):
        while True:
            try:
                await ws.send_json({"req_id": "100001", "op": "ping"})
                await asyncio.sleep(20)  # Pause between pings
            except Exception as e:
                await self.on_error(ws, e)
                break  # Exit the loop to stop sending pings

    async def on_open(self, ws):
        print(ws, 'Websocket was opened')

        # Запуск асинхронного отправления heartbeat
        asyncio.create_task(self.send_heartbeat(ws))

        # Подписка на топики:
        data = {"op": "subscribe", "args": self.params}
        await ws.send_json(data)

    async def on_error(self, ws, error):
        print('on_error', ws, error)
        print(traceback.format_exc())

    async def on_message(self, ws, msg):
        data = json.loads(msg.data)
        print(data)
        # await asyncio.sleep(20)



if __name__ == '__main__':

    async def custom_on_message(ws, msg):
        try:
            data = json.loads(msg.data)
            if 'data' in data and data.get('data')[0].get('confirm') is True:
                print("Custom message handler:", data.get('topic'), '\n', data.get('data')[0])
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON in custom handler: {e}")

    url_spot = 'wss://stream.bybit.com/v5/public/spot'
    url_futures = 'wss://stream.bybit.com/v5/public/linear'

    topics = [
        'kline.1.BTCUSDT',
        'kline.1.AXSUSDT',
    ]

    socket = SocketBybit(url_futures, topics, on_message=custom_on_message)
    asyncio.run(socket.connect())