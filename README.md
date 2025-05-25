## Task Description

This repository contains a Spark Structured Streaming application that implements the following steps:

1. **Data stream generation**  
   - Input data is read from a Kafka topic (`oleg_building_sensors`).  
   - Each record includes `sensor_id`, `temperature`, `humidity`, and `timestamp`.  

2. **Data aggregation**  
   - Consume the stream from step 1.  
   - Using a sliding window of 1 minute (slide = 30 seconds) with a 10-second watermark, compute average temperature and humidity.  

3. **Alert parameters loading**  
   - Load alert definitions from `alerts_conditions.csv`.  
   - The CSV defines min/max thresholds for temperature and humidity, plus an alert code and message.  
   - A value of `-999` indicates that a threshold is not applied for that alert.  

4. **Alert detection**  
   - Cross-join the windowed averages with alert definitions and filter by each alert’s criteria.  

5. **Write alerts to Kafka**  
   - Publish each triggered alert as a JSON message to the `alerts_output` Kafka topic.  
   - The JSON includes window start/end, averages, alert code, message, and a processing timestamp.
