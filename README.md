# Project Setup


## 1. Install Java
Make sure Java 17 is installed and `JAVA_HOME` is set:

```bash
java -version
echo $JAVA_HOME
```
## 2. Install Python dependencies 
Create a virtual environment and install the required packages:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
## 3. Install Apache Spark

Extract Spark and set the environment variables:

```bash
tar -xzf spark-3.5.3-bin-hadoop3.tgz

export SPARK_HOME=$(pwd)/spark-3.5.3-bin-hadoop3
export PATH=$SPARK_HOME/bin:$PATH
```
## 4. Run the pipeline

Run the main Spark pipeline script:

```bash
python3 instacart_spark_pipeline.py
```

