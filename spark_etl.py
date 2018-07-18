from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def get_lang_type(language):
    """
    Accepts a String of the programming language and returns the language type.
    """

    procedural = ['Basic', 'C']
    object_oriented = ['C#', 'C++', 'Java', 'Python']
    functional = ['Lisp', 'Haskell', 'Scala']
    data_science = ['R', 'Jupyter Notebook', 'Julia']

    # Check if language string exists to avoid null.
    if language:
        if language in procedural:
            type = "Procedural"
        elif language in object_oriented:
            type = "Object Oriented"
        elif language in functional:
            type = "Functional"
        elif language in data_science:
            type = "Data Science"
        else:
            type = "Others"
        return type
    else:
        return None


def set_lang_type(df, col_name):
    """
    Accepts DataFrame and applies get_lang_type() to the column containing the
    list of repo languages. Appends new column containing the type of language
    and returns the DataFrame.
    """

    # Register as user defined function, output String.
    get_lang_type_udf = udf(get_lang_type, StringType())

    return df.withColumn(col_name, get_lang_type_udf(df['pr_repo_language']))


def save_to_parquet(df, filename):
    # Default compression is snappy.
    return df.write.save(filename, format="parquet")


def main(spark):
    """
    Reads in JSON and gets table. Applies required processing and saves results.
    """

    # Read JSON file and register DataFrame as a SQL temporary view.
    github_json = spark.read.json("2017-10-01-10.json.gz")
    github_json.createOrReplaceTempView('GithubEvents')
    # SQL query to select required info.
    pull_requests = spark.sql("SELECT \
                                created_at AS created_as, \
                                repo.name AS repo_name, \
                                actor.login AS username, \
                                payload.pull_request.user.login AS pr_username, \
                                payload.pull_request.created_at AS pr_created_at, \
                                payload.pull_request.head.repo.language AS pr_repo_language \
                               FROM GithubEvents WHERE type='PullRequestEvent'")
    # Set repo languae type.
    pull_requests_type = set_lang_type(pull_requests, 'pr_repo_language_type')
    pull_requests_type.show()
    # Save to parquet.
    save_to_parquet(pull_requests_type, "github_pull_requests_parquet")


if __name__ == '__main__':
    # Create SparkSession
    spark = SparkSession.builder.appName('Github Pull Requests').getOrCreate()
    main(spark)
