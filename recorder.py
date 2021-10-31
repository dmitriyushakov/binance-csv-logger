import asyncio
import yaml
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper
import itertools as itt
import click

from receiver import BinanceWSReceiverProvider
from entities import DepthUpdate, KLine, Trade
from event_writer import AsyncCSVWritersContext
from utils import execute_application_loop

async def iterate_over_book_order_events(path_prefix,receiver,api_type,symbol,gzip_enabled,writers):
    gz_suffix = '.gz' if gzip_enabled else ''
    book_order_writer = await writers.open(DepthUpdate,path_prefix + symbol + '_' + api_type + '_book_order-%Y-%m-%d.csv' + gz_suffix, use_gzip = gzip_enabled)
    async for event in receiver.subscribe_book_order(symbol):
        await book_order_writer.put_event(event)

async def iterate_over_klines(path_prefix,receiver,api_type,symbol,interval,gzip_enabled,writers):
    gz_suffix = '.gz' if gzip_enabled else ''
    kline_writer = await writers.open(KLine,path_prefix + symbol + '_' + api_type + '_kline_' + interval + '-%Y-%m-%d.csv' + gz_suffix, use_gzip = gzip_enabled)
    async for event in receiver.subscribe_klines(symbol,interval):
        await kline_writer.put_event(event)

async def iterate_over_trades(path_prefix,receiver,api_type,symbol,gzip_enabled,writers):
    gz_suffix = '.gz' if gzip_enabled else ''
    trade_writer = await writers.open(Trade,path_prefix + symbol + '_' + api_type + '_trade-%Y-%m-%d.csv' + gz_suffix, use_gzip = gzip_enabled)
    async for event in receiver.subscribe_trades(symbol):
        await trade_writer.put_event(event)

def load_config(filename):
    with open(filename) as yaml_config_file:
        yaml_config = yaml.load(yaml_config_file,Loader = Loader)
        return yaml_config

def start_recording_tasks_by_config(loop,receiver_provider,config,writers):
    if type(config) is dict:
        config = [config]
    
    for rec_group in config:
        symbols = rec_group['symbols']
        klines_intervals = rec_group.get('klines_intervals',[])
        rec_book_order_updates = rec_group.get('rec_book_order_updates',False)
        rec_klines = rec_group.get('rec_klines',False)
        rec_trades = rec_group.get('rec_trades',False)
        gzip_enabled = rec_group.get('gzip_enabled',False)
        folder_path = rec_group.get('folder')
        api_types = rec_group.get('api_types',['spot'])

        if folder_path is None:
            path_prefix = './'
        else:
            path_prefix = folder_path
            if not path_prefix[-1] == '/':
                path_prefix = path_prefix + '/'

        if rec_book_order_updates:
            for sym,api_type in itt.product(symbols,api_types):
                receiver = receiver_provider.get(api_type)
                loop.create_task(iterate_over_book_order_events(path_prefix,receiver,api_type,sym,gzip_enabled,writers))
        
        if rec_klines:
            for sym,interval,api_type in itt.product(symbols,klines_intervals,api_types):
                receiver = receiver_provider.get(api_type)
                loop.create_task(iterate_over_klines(path_prefix,receiver,api_type,sym,interval,gzip_enabled,writers))
        
        if rec_trades:
            for sym,api_type in itt.product(symbols,api_types):
                if api_type != 'spot':
                    continue
                receiver = receiver_provider.get(api_type)
                loop.create_task(iterate_over_trades(path_prefix,receiver,api_type,sym,gzip_enabled,writers))
        
async def start_binance_events_recording(writers,config_filename):
    receiver_provider = BinanceWSReceiverProvider()
    loop = asyncio.get_running_loop()
    
    config = load_config(config_filename)
    start_recording_tasks_by_config(loop, receiver_provider, config, writers)
    
    receiver_provider.start()

@click.command()
@click.option('--config',default = 'config.yaml')
def start_recorder(config):
    with AsyncCSVWritersContext() as writers:
        coro = start_binance_events_recording(writers,config)
        execute_application_loop(coro)

if __name__ == '__main__':
    start_recorder()