#!/usr/bin/env python3
import asyncio
import sys
import argparse
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

async def main():
    parser = argparse.ArgumentParser(description="Ensure Kafka topics exist.")
    parser.add_argument("--brokers", default="127.0.0.1:39093", help="Comma-separated list of Kafka brokers")
    parser.add_argument("--partitions", type=int, default=1, help="Number of partitions for the new topics")
    parser.add_argument("topics", nargs="+", help="Topics to create")
    
    args = parser.parse_args()
    
    admin_client = AIOKafkaAdminClient(bootstrap_servers=args.brokers)
    await admin_client.start()
    
    try:
        new_topics = [NewTopic(name=topic, num_partitions=args.partitions, replication_factor=1) for topic in args.topics]
        await admin_client.create_topics(new_topics)
        print(f"✅ Successfully ensured topics exist: {', '.join(args.topics)}")
    except TopicAlreadyExistsError:
        pass
    except Exception as e:
        print(f"Failed to create topics: {e}", file=sys.stderr)
        # We don't fail completely here; some topics might have been created or already exist.
        # aiokafka might raise a combined error if some exist and some don't. Let's just log it.
        if "TopicAlreadyExistsError" not in str(e):
            sys.exit(1)
    finally:
        await admin_client.close()

if __name__ == "__main__":
    asyncio.run(main())
