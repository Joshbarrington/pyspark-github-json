"""
Unit test using pytest-spark see: https://pypi.org/project/pytest-spark/
which defines session scope fixture spark_context which is passed to tests.
Example usage at command line: $ pytest unit_tests.py
"""


import pyspark
import pyspark.sql
import pandas as pd
from pandas.util.testing import assert_frame_equal
from spark_etl import set_lang_type


def test_set_lang_type(spark_session):
    """
    Test set_lang_type function.
    """

    # Example input dataframe.
    df = pd.DataFrame({'pr_repo_language':[ 'Basic', 'C', 'C#', 'C++', 'Java',
                                            'Python', 'Lisp', 'Haskell', 'Scala',
                                            'R', 'Jupyter Notebook', 'Julia',
                                            'Javascript']})

    # Expected types.
    type_df = pd.DataFrame({'pr_repo_language_type':['Procedural', 'Procedural',
                                                     'Object Oriented', 'Object Oriented',
                                                     'Object Oriented', 'Object Oriented',
                                                     'Functional', 'Functional', 'Functional',
                                                     'Data Science', 'Data Science',
                                                     'Data Science', 'Others']})
    # Expected df has added column.
    expected_df = df.join(type_df)
    # Convert to Spark DataFrame.
    test_df = spark_session.createDataFrame(df)
    # Convert back to pandas.
    func_df = set_lang_type(test_df, 'pr_repo_language_type').toPandas()
    # Assert equality between the two DataFrames.
    assert_frame_equal(func_df, expected_df)
