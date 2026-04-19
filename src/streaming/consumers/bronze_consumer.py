class BronzeConsumer:

    def __init__(self, consumer, storage, validator, buffer):
        self.consumer = consumer
        self.storage = storage
        self.validator = validator
        self.buffer = buffer
    
    def run(self):
      for message in self.consumer.consume():

          event = message.value

          valid, reason = self.validator.validate(event)

          if not valid:
              self.handle_invalid(event, reason)
              continue

          self.enrich(event, message)

          self.buffer.add(event)

          if self.buffer.should_flush():
              self.flush()
              self.consumer.commit()
    
    def enrich(self, event, message):
      event["_kafka_offset"] = message.offset
    
    def flush(self):
      snapshot = self.buffer.drain()

      for partition, events in snapshot.items():
          key = self.build_key(partition)

          data = "\n".join(json.dumps(e) for e in events).encode("utf-8")

          self.storage.put_object(key, data)