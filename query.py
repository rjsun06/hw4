from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, col, when, split
import re

app = Flask(__name__)

# Initialize Spark session
spark = SparkSession.builder.appName("SearchAnalytics").getOrCreate()

# Load preprocessed data
df = spark.read.json("processed_data")

# Cache the DataFrame for faster queries
df.cache()

def extract_domain_type(url):
    """Extract domain suffix and return priority"""
    domain = split(url, "\.").getItem(-1)
    # domain = url.split('.')[-1].lower()
    return when(domain == 'org', 1)\
           .when(domain == 'edu', 2)\
           .when(domain == 'com', 3)\
           .otherwise(4)

@app.route('/results', methods=['POST'])
def get_results():
    data = request.json
    search_term = data.get('term', '').strip()
    
    result_df = df.filter(col("term") == search_term)\
        .groupBy("url")\
        .agg(spark_sum("clicks").alias("clicks"))\
        .withColumn("domain_priority", extract_domain_type(col("url")))\
        .orderBy(col("clicks").desc(), col("domain_priority").asc())
    results = {row.url: row.clicks for row in result_df.collect()}
    app.json.sort_keys = False
    results = dict(sorted(results.items(), key=lambda item: item[l], reverse=True)
    return jsonify("results": results} )

@app.route('/trends', methods=['POST'])
def get_trends():
    data = request.json
    search_term = data.get('term', '').strip()
    
    total_clicks = df.filter(col("term") == search_term)\
        .agg(spark_sum("clicks")).first()[0] or 0
    
    return jsonify({"clicks": total_clicks})

@app.route('/popularity', methods=['POST'])
def get_popularity():
    data = request.json
    url = data.get('url', '').strip()
    
    total_clicks = df.filter(col("url") == url)\
        .agg(spark_sum("clicks")).first()[0] or 0
    
    return jsonify({"clicks": total_clicks})

@app.route('/getBestTerms', methods=['POST'])
def get_best_terms():
    data = request.json
    website = data.get('website', '').strip()
    
    # Calculate total clicks for the website
    total_clicks = df.filter(col("url") == website)\
        .agg(spark_sum("clicks")).first()[0] or 0
    
    if total_clicks == 0:
        return jsonify({"best_terms": []})
    
    # Find terms contributing more than 5% of total clicks
    best_terms = df.filter(col("url") == website)\
        .groupBy("term")\
        .agg(spark_sum("clicks").alias("term_clicks"))\
        .withColumn("percentage", col("term_clicks") / total_clicks)\
        .filter(col("percentage") > 0.05)\
        .select("term")\
        .rdd.flatMap(lambda x: x).collect()
    
    return jsonify({"best_terms": best_terms})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
