
class dataProcessHandler:
    def __init__(self):
        pass

class dataProcessSparkHandler():
    def __init__(self, cores="4", memory="20Gi", mount_path="/root"):
        from sparkdriver import K8sSparkDriver
        self.driver = K8sSparkDriver(cpu=cores, memory=memory, mount_path=mount_path, remote=True)
        self.spark = None

    # def createDriver():
    #     pass
    
    def getDriver(self):
        return self.driver

    def getSpark(self):
        return self.spark

    def createExecutor(self, instances="2", memory="15g", cores="5"):
        config = {
            "spark.executor.instances": instances,
            "spark.executor.memory": memory,
            "spark.executor.cores": cores,
            # "spark.driver.memory": "10g",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3.impl": "com.amazon.ws.emr.hadoop.fs.EmrFileSystem",
            "spark.hadoop.fs.s3n.impl": "com.amazon.ws.emr.hadoop.fs.EmrFileSystem",
            "spark.hadoop.fs.s3bfs.impl": "org.apache.hadoop.fs.s3.S3FileSystem",
            "spark.hadoop.fs.s3.buffer.dir": "/opt/mnt/s3",
            "spark.executorEnv.SPARK_USER": "root",
            'spark.kubernetes.namespace': "spark-operator",
            "spark.kubernetes.node.selector.alpha.eksctl.io/nodegroup-name": "ng-memory-5-spark",
        }
        sparkContext = self.driver.getSparkContext(config)

        from pyspark.sql import SparkSession
        self.spark = SparkSession(sparkContext)
    
    def executQuery(self, query):
        result = self.spark.sql(query).collect()
        print(result)
        return result