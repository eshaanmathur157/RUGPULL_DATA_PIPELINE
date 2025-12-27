import redis

def initialize_redis_data():
    # --- Configuration ---
    REDIS_HOST = 'localhost'
    REDIS_PORT = 6379
    REDIS_DB = 0

    # Key names
    KEYS = {
        # Existing Sets
        "base_vaults": "BASE_VAULTS",
        "quote_vaults": "QUOTE_VAULTS",
        "base_mints": "BASE_MINTS",
        "quote_mints": "QUOTE_MINTS",
        
        # NEW: Hash Maps
        "pair_to_base": "PAIR_TO_BASE_VAULT",   # Map: Pair Address -> Base Vault
        "pair_to_quote": "PAIR_TO_QUOTE_VAULT"  # Map: Pair Address -> Quote Vault
    }

    # --- SOURCE DATA FROM YOUR JSON ---
    # Structure:
    # id = Pair Address
    # vault.B = Base Vault (holds the volatile token like Unipcs, ROCK, FKH)
    # vault.A = Quote Vault (holds USD1)
    # mintB = Base Mint
    # mintA = Quote Mint
    
    pools_data = [
        # Pool 1: Unipcs / USD1
        {
            "pair_id": "8P2kKPp3s38CAek2bxALLzFcooZH46X8YyLckYp6UkVt",
            "base_vault": "5BCZRRPXi41SdzdvhghxG7NHw5SiaZsSdLwT7n6CAMt3",
            "quote_vault": "DVLdDa689zwWCHVBZHmaVqaKM7LyfuMEeEuQ1QsauqZT",
            "base_mint": "2orNgazHWM1f2g2KKKsLqGZEnH4vtJ2iMhAYCW6M5SnV", 
            "quote_mint": "USD1ttGY1N17NEEHLmELoaybftRBUSErhqYiQzvEmuB"
        },
        # Pool 2: ROCK / USD1
        {
            "pair_id": "CTDpCZejs8oi4dwGNqYZgHxr8GRj86PSMGsAz3cgKPYq",
            "base_vault": "HLp1oNYmEXUoEzxrG9Cxxtg74WLALZgquk29W9haps8L",
            "quote_vault": "13FcrjpmAftTfhAUqCpU82FvDbthgBWXyQvFWT2McQ8K",
            "base_mint": "D8FYTqJGSmJx2cchFDUgzMYEe4VDvUyGWAYCRJ4Xbonk",
            "quote_mint": "USD1ttGY1N17NEEHLmELoaybftRBUSErhqYiQzvEmuB"
        },
        # Pool 3: FKH / USD1
        {
            "pair_id": "8Lq7gz2aEzkMQNfLpYmjv3V8JbD26LRbFd11SnRicCE6",
            "base_vault": "FcdVJiinzBd7s3nBh6v7hNduweeRirRGHXrD1rQQP89",
            "quote_vault": "Cunr3MeYP28JqyNcQTD3e2Kjz7FJH7E3mUFxKob47JmH",
            "base_mint": "BCXpjsHYmgVpRKdv4EQv1RARhYagnnwPkJjYbvM6bonk",
            "quote_mint": "USD1ttGY1N17NEEHLmELoaybftRBUSErhqYiQzvEmuB"
        }
    ]

    try:
        # Connect to Redis
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        print(f"✓ Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")

        pipe = r.pipeline()

        # 1. Clear ALL existing keys to ensure a fresh start
        print("→ Cleaning old keys...")
        for key_name in KEYS.values():
            pipe.delete(key_name)

        # 2. Prepare Data Structures
        base_vaults_set = set()
        quote_vaults_set = set()
        base_mints_set = set()
        quote_mints_set = set()
        
        # Dictionaries for the new Hash Maps
        pair_to_base_map = {}
        pair_to_quote_map = {}

        # 3. Iterate through source data
        for pool in pools_data:
            # Add to Sets
            base_vaults_set.add(pool["base_vault"])
            quote_vaults_set.add(pool["quote_vault"])
            base_mints_set.add(pool["base_mint"])
            quote_mints_set.add(pool["quote_mint"])
            
            # Add to Maps (Pair ID -> Vault)
            pair_to_base_map[pool["pair_id"]] = pool["base_vault"]
            pair_to_quote_map[pool["pair_id"]] = pool["quote_vault"]

        # 4. Pipeline Commands
        
        # Populate Sets (Existing functionality)
        if base_vaults_set: pipe.sadd(KEYS["base_vaults"], *base_vaults_set)
        if quote_vaults_set: pipe.sadd(KEYS["quote_vaults"], *quote_vaults_set)
        if base_mints_set: pipe.sadd(KEYS["base_mints"], *base_mints_set)
        if quote_mints_set: pipe.sadd(KEYS["quote_mints"], *quote_mints_set)

        # Populate Hash Maps (New functionality)
        if pair_to_base_map: pipe.hset(KEYS["pair_to_base"], mapping=pair_to_base_map)
        if pair_to_quote_map: pipe.hset(KEYS["pair_to_quote"], mapping=pair_to_quote_map)

        # 5. Execute
        pipe.execute()
        print("✓ Data populated successfully.")

        # --- Verification ---
        print("\n--- Current Redis State ---")
        
        # Verify Sets
        set_keys = ["base_vaults", "quote_vaults"]
        for k in set_keys:
            key_name = KEYS[k]
            count = r.scard(key_name)
            print(f"[{key_name}] Count: {count}")

        # Verify Hash Maps
        map_keys = ["pair_to_base", "pair_to_quote"]
        for k in map_keys:
            key_name = KEYS[k]
            data = r.hgetall(key_name)
            print(f"\n[{key_name}] Items: {len(data)}")
            for pair, vault in data.items():
                print(f"  Pair: {pair[:15]}... -> Vault: {vault[:15]}...")

    except redis.ConnectionError:
        print("✗ Could not connect to Redis. Is the server running?")
    except Exception as e:
        print(f"✗ An error occurred: {e}")

if __name__ == "__main__":
    initialize_redis_data()
