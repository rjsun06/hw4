
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
def parse_line(line):
    if not line.startswith("searchTerm: "):
        return []
    
    try:
        # Split term and URL-click pairs using comma (ignore space)
        content = line[len("searchTerm: "):].split(',', 1)  # Split on FIRST comma
        term_part = content[0].strip().strip('‘’')  # Remove curly quotes
        pairs_part = content[1].strip() if len(content) > 1 else ""
    except ValueError:
        return []
    
    records = []
    for pair in pairs_part.split('~'):
        pair = pair.strip()
        if not pair:
            continue
        parts = pair.split(':', 1)
        if len(parts) != 2:
            continue
        url, click_str = parts[0].strip(), parts[1].strip()
        try:
            clicks = int(click_str)
            records.append((term_part, url, clicks))
        except ValueError:
            continue
    
    return records

spark = SparkSession.builder.appName("Preprocess").getOrCreate()

text_file = spark.sparkContext.textFile("searchLog.csv")

parsed = text_file.flatMap(parse_line)

schema = StructType([
    StructField("term", StringType(), False),
    StructField("url", StringType(), False),
    StructField("clicks", IntegerType(), False)
])

df = spark.createDataFrame(parsed, schema=schema)
df.write.json("processed_data", mode="overwrite")

spark.stop()