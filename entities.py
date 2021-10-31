from collections import namedtuple

BID = 'BID'
ASK = 'ASK'

DepthUpdate = namedtuple('DepthUpdate',['time','symbol','side','price','qty','last_update_id'])
KLine = namedtuple('KLine',['time','symbol','start_time','close_time','interval','open_price','close_price','high_price','low_price','base_asset_volume','num_of_trades','quote_asset_volume','taker_buy_base_asset_volume','taker_buy_quote_asset_volume'])
Trade = namedtuple('Trade',['time','symbol','trade_id','price','quantity','buyer_order_id','seller_order_id','trade_time','is_buyer_market_maker'])