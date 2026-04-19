"""seed_storage/ingestion — Discord ingestion adapters.

Real-time ingestion via bot.py and batch import via batch.py.
Both produce raw_payload (Contract 1) and enqueue via enrich_message.delay().
"""
