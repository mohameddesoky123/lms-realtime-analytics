# contracts/

The shared agreements of the team (project plan, Section 4). **Changing anything here needs a Pull Request approved by all 6 members.**

| File | Owner | Used by |
|------|-------|---------|
| `sample_raw.jsonl`   | Member 1 | Member 2 - send to `lms_raw` to test NiFi |
| `sample_clean.jsonl` | Member 1 | Members 3, 4, 5 - send to `lms_clean` to test Spark |

Send a sample to Kafka:

```bash
docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic lms_clean < contracts/sample_clean.jsonl
```

Check a file against the contract: `python 1_producer/validate_contract.py contracts/sample_clean.jsonl`
