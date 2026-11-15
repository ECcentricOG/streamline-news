class SparkStorageClient:
    def __init__(self, spark) -> None:
        self.spark = spark

    def exists(self, path:str) -> bool:
        if not path:
            raise ValueError("Path cannot be empty")

        jvm = self.spark._jvm
        hadoop_conf = (
            self.spark.sparkContext
            ._jsc.hadoopConfiguration()
        )

        hadoop_path = jvm.org.apache.hadoop.fs.Path(str(path))

        filesystem = hadoop_path.getFileSystem(hadoop_conf)
        return filesystem.exists(hadoop_path)
