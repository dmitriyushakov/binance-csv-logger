import asyncio
import os.path
from concurrent.futures import ThreadPoolExecutor
from csv import DictWriter
from datetime import datetime
import gzip
import traceback

class NamedTupleCSVWriter:
    def __init__(self,tuple_type,filename_mask,use_gzip = False):
        self._fields = list(tuple_type._fields)
        self._last_filename = None
        self._filename_mask = filename_mask
        self._file_opened = None
        self._dict_writer = None
        self._header_written = False
        self._use_gzip = use_gzip
    
    def _ensure_file_opened(self):
        dt = datetime.now()
        actual_filename = dt.strftime(self._filename_mask)
        if self._file_opened is None or actual_filename != self._last_filename:
            self._last_filename = actual_filename
            self._header_written = os.path.exists(actual_filename)
            
            if not self._file_opened is None:
                self._file_opened.close()
            if self._use_gzip:
                self._file_opened = gzip.open(actual_filename,'at')
            else:
                self._file_opened = open(actual_filename,'a')
            
            self._dict_writer = DictWriter(self._file_opened, self._fields)
    
    def put_event(self,tuple):
        self._ensure_file_opened()
        row_contents = dict()
        for field in self._fields:
            row_contents[field] = getattr(tuple,field)
        
        if not self._header_written:
            self._header_written = True
            self._dict_writer.writeheader()
        
        self._dict_writer.writerow(row_contents)
    
    def __call__(self,tuple):
        self.put_event(tuple)
    
    def close(self):
        if not self._file_opened is None:
            self._file_opened.close()
            self._file_opened = None

class CSVWritersContext:
    def __init__(self):
        self._accounted_writers = list()
    
    def open(self,tuple_type,filename_mask,use_gzip = False):
        writer = NamedTupleCSVWriter(tuple_type, filename_mask, use_gzip = use_gzip)
        self._accounted_writers.append(writer)

        return writer
    
    def close(self,writer):
        writer.close()
        self._accounted_writers.remove(writer)
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tcb):
        for writer in self._accounted_writers:
            try:
                writer.close()
            except:
                traceback.print_exc()

class AsyncNamedTupleCSVWriter:
    def __init__(self,wrapped_writter,executor = None):
        self._blocking_writer = wrapped_writter
        self._executor = executor
    
    async def put_event(self,tuple):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor,lambda:self._blocking_writer.put_event(tuple))
    
    async def close(self):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor,lambda:self._blocking_writer.close())

class AsyncCSVWritersContext:
    def __init__(self):
        self._blocking_writers_context = CSVWritersContext()
        self._executor = ThreadPoolExecutor(thread_name_prefix = "CSV Writer Threads")
    
    async def open(self,tuple_type,filename_mask,use_gzip = False):
        blocking_writer = self._blocking_writers_context.open(tuple_type,filename_mask,use_gzip)
        return AsyncNamedTupleCSVWriter(blocking_writer,self._executor)
    
    async def close(self,writer):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor,self._blocking_writers_context.close(writer._blocking_writer))
    
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, tcb):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor,lambda:self._blocking_writers_context.__exit__(exc_type, exc_value, tcb))
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tcb):
        self._blocking_writers_context.__exit__(exc_type, exc_value, tcb)