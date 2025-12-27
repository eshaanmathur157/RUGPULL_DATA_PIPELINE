#here we create a script to ingest prices into a queue as a json string

import json 
import queue
import requests
import redis
#lets create a map, of pair -> list (base price, quote price)
hashmap = {}

#each event is a json, of the form {"pair": address, "baseprice": baseprice, "quoteprice": quoteprice}
#send the event to a redis channel: prices_channel

pair_list = ["8P2kKPp3s38CAek2bxALLzFcooZH46X8YyLckYp6UkVt",
            "CTDpCZejs8oi4dwGNqYZgHxr8GRj86PSMGsAz3cgKPYq",
            "8Lq7gz2aEzkMQNfLpYmjv3V8JbD26LRbFd11SnRicCE6"]

#for each pair, lets request and see how much of change has happened?
while True:
    for pair in pair_list:
        url = f"https://api.dexscreener.com/latest/dex/pairs/solana/{pair}"
        response = requests.get(url, timeout=5)

        if response.status_code == 200:
                    data = response.json()
                    pairs = data.get("pairs", [])
                    
                    if pairs:
                        pair_data = pairs[0]
                        base_token = pair_data.get("baseToken", {})
                        quote_token = pair_data.get("quoteToken", {})
                        
                        # we fetch the prices, use the amm math and fetch base and quote price
                        base_price_usd_str = pair_data.get("priceUsd", "0")
                        base_price_usd = float(base_price_usd_str) if base_price_usd_str else 0.0
                        price_native_str = pair_data.get("priceNative", "0")
                        price_native = float(price_native_str) if price_native_str else 0.0

                        quote_price_usd = 0.0
                        if price_native > 0:
                            quote_price_usd = base_price_usd / price_native

                        #check if pair in hashmap or at least one of base or quote price is greater than 10% of the original price in the hashmap
                        if pair in hashmap:
                            old_base_price, old_quote_price = hashmap[pair]
                            if (abs(base_price_usd - old_base_price) / old_base_price > 0.1) or (abs(quote_price_usd - old_quote_price) / old_quote_price > 0.1):
                                #update the hashmap
                                hashmap[pair] = (base_price_usd, quote_price_usd)
                                #create the event
                                event = {"pair": pair, "baseprice": base_price_usd, "quoteprice": quote_price_usd}
                                #send the event to the redis prices channel
                                r = redis.Redis(host='20.46.50.39', port=6379, db=0)
                                r.publish('prices_channel', json.dumps(event))
                                print(f"Published event for pair {pair}: {event}")
                            else:
                                print(f"No significant change for pair {pair}. Skipping event.")
                        else:
                            #first time seeing this pair, add to hashmap and send event
                            hashmap[pair] = (base_price_usd, quote_price_usd)
                            event = {"pair": pair, "baseprice": base_price_usd, "quoteprice": quote_price_usd}
                            r = redis.Redis(host='20.46.50.39', port=6379, db=0)
                            r.publish('prices_channel', json.dumps(event))
                            print(f"Published event for pair {pair}: {event}")
        time.sleep(1)