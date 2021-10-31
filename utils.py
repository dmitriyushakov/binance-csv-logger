import threading
import asyncio
from signal import SIGTERM
import time

class IntSequence:
    def __init__(self):
        self._counter = 0
        self._lock = threading.Lock()
    
    def next(self):
        self._lock.acquire()
        res = self._counter
        self._counter = self._counter + 1
        self._lock.release()

        return res
    
    def __next__(self):
        return self.next()
    
    def __iter__(self):
        return self
    
    def __call__(self):
        return self.next()


class AsyncIterQueue:
    def __init__(self):
        self._queue = asyncio.Queue()
        self._raised_exception = None
        self._is_exception_raised = False
    
    def __aiter__(self):
        return self
    
    async def __anext__(self):
        value = await self._queue.get()
        if self._is_exception_raised:
            self._is_exception_raised = False
            raise self._raised_exception
        return value
    
    def send(self,val):
        self._queue.put_nowait(val)
    
    def send_raise_exception(self,exception):
        self._raised_exception = exception
        self._is_exception_raised = True
        self._queue.put_nowait(None)
    
    def stop_iteration(self):
        self.send_raise_exception(StopAsyncIteration)

def execute_application_loop(coro):
    def raise_keyboard_interrupt():
        raise KeyboardInterrupt
    
    async def other_corutines_finished_waiter(timeout = None):
        begin_time = time.time()
        while True:
            this_task = asyncio.current_task()
            found = False
            for task in asyncio.all_tasks():
                if task != this_task:
                    found = True
                    break
            
            if found:
                await asyncio.sleep(1)
            else:
                break

            if not timeout is None and time.time() - begin_time > timeout:
                print("Force shutdown by timeout")
                break
    
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(SIGTERM,raise_keyboard_interrupt)
    loop.create_task(coro)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        for task in asyncio.all_tasks(loop):
            task.cancel()

        success = False
        while not success:
            success = True
            waiter = None
            try:
                waiter = loop.create_task(other_corutines_finished_waiter())
                loop.run_until_complete(waiter)
            except KeyboardInterrupt:
                success = False
                if not waiter is None:
                    waiter.cancel()