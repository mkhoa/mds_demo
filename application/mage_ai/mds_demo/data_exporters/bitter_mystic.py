if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    # Specify your data exporting logic here
    columns = ["language","users_count"]
    data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
    spark = kwargs.get('spark')
    rdd = spark.sparkContext.parallelize(data)
    dfFromRDD1 = rdd.toDF(columns)
    dfFromRDD1.createOrReplaceTempView("mytempTable") 
    dfFromRDD1.show()
    spark.sql("DROP TABLE IF EXISTS tempTable")
    spark.sql("CREATE TABLE IF NOT EXISTS tempTable AS SELECT * FROM mytempTable")