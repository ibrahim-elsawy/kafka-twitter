import asyncio
import websockets
from pysentimiento import create_analyzer
import torch


analyzer = create_analyzer(task="sentiment", lang="en")


# ANSI colors
c = [
	"\033[0m ",   # End of color ---> 0
	"\033[36m ",  # Cyan        ---> 1
	"\033[91m ",  # Red        ---> 2
	"\033[35m ",  # Magenta  ---> 3
	"\033[32m ",  # Green    ---> 4
	"\033[33m ",   # Yellow    ---> 5
	"\033[34m "   # Blue       ---> 6
]

async def processRequest(websocket): 
	msg = await websocket.recv()
	print(c[2] + "recieved a message from " + c[0]+ c[1]+websocket.remote_address[0]+"\n"+c[0])
	data = analyzer.predict(msg).output
	print(c[4]+"the output is " + c[0] + c[3] + data +c[0])
	torch.cuda.empty_cache()
	await websocket.send(data)




async def main(): 
	async with websockets.serve(processRequest, "localhost", 8765): 
		await asyncio.Future()  # run forever


if __name__ == "__main__": 
	asyncio.run(main())