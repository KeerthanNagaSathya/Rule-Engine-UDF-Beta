import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import logging.config
from pyspark.sql.functions import *
import datetime


def ingest_config():
    json_df = spark.read.option("multiline", "true").json("data/rules.json")
    logging.info("reading test json from file")
    # logging.info(json_df.printSchema())
    return json_df


def ingest_atm_file():
    # Reading the source atm file and loading into a dataframe
    atm_df = spark.read.option("Header", "true").option("InferSchema", "true").csv("data/atm.csv")
    logging.info("Reading atm transactions csv file")
    return atm_df


def parse_json(json_df):
    logging.info("Parsing Json")

    rulesExplodedDf = json_df.select(explode("rules").alias("rules")).select("rules.*")
    # logging.info(rulesExplodedDf.printSchema())
    # logging.info(rulesExplodedDf.show(truncate=False))

    parentDf = rulesExplodedDf.select("id", "name", "description", "is_valid", "valid_from", "valid_till",
                                      explode("then").alias("then")) \
        .select("id", "name", "description", "is_valid", "valid_from", "valid_till", "then.*")
    # logging.info(parentDf.printSchema())
    # logging.info(parentDf.show(truncate=False))

    childDf = rulesExplodedDf.select("id", explode("when").alias("when")) \
        .select("id", "when.*")
    # logging.info(childDf.printSchema())
    # logging.info(childDf.show(truncate=False))

    return parentDf, childDf


def rules_pipeline(pdf, cdf, table_name):
    rule_success = False
    check_rule = False
    check_rule_id = 0
    where_query = " where"
    select_query = """ with atm_window as (select id, date, txn_source_code, city, amount, is_ttr, time, sum(amount) over (partition by id, date, txn_source_code order by date) as total_amount, max(time) over (partition by id, date, txn_source_code order by date) as max_time, min(time) over (partition by id, date, txn_source_code order by date) as min_time from {}) select * from atm_window""".format(
        table_name)

    for i in pdf:

        logging.info("Looping through the json list")

        if check_rule:
            where_query = where_query + " and"
            logging.info("where_query > {}".format(where_query))

        p_id = i["id"]
        p_name = i["name"]
        p_desc = i["description"]
        p_is_valid = i["is_valid"]
        p_valid_from = i["valid_from"]
        p_valid_till = i["valid_till"]
        p_field_name = i["field_name"]
        p_field_value = i["field_value"]

        if p_is_valid == "true":
            if p_valid_till != 1:  # This needs to be replaced with if current date is in between valid from and
                # valid till
                logging.info("Rule {} is valid and is being checked".format(p_id))

                for j in cdf:
                    # print(row["field_name"] + ' > ' + row["field_value"] + ' > ' + row["join"] + ' > ' + row[
                    # "operator"] + ' > ')
                    c_id = j["id"]
                    c_name = j["field_name"]
                    c_value = j["field_value"]
                    c_join = j["join"]
                    c_operator = j["operator"]

                    if check_rule:
                        validate_rule_id = check_rule_id
                        logging.info("check_rule is true and validate_rule_id is {}".format(validate_rule_id))
                    else:
                        validate_rule_id = int(p_id)
                        logging.info(
                            "check_rule is false therefore follow the queue and validate_rule_id is {}".format(
                                validate_rule_id))

                    if int(c_id) == validate_rule_id:

                        if not (j["join"] and j["join"].strip()) != "":

                            logging.info("Join is empty")

                            if c_value.isnumeric():
                                c_value = int(c_value)
                                '''df.filter(col("state") == = "OH")'''
                                if c_operator == "LIKE" or c_operator == "NOT LIKE":
                                    where_query = where_query + " {} {} {}%".format(c_name, c_operator, c_value)
                                else:
                                    where_query = where_query + " {} {} {}".format(c_name, c_operator, c_value)
                                logging.info("query > {}".format(where_query))
                            else:
                                '''df.filter(col("state") == = "OH")'''
                                if c_operator == "LIKE" or c_operator == "NOT LIKE":
                                    where_query = where_query + " {} {} '{}%'".format(c_name, c_operator, c_value)
                                else:
                                    where_query = where_query + " {} {} '{}'".format(c_name, c_operator, c_value)
                                logging.info("query > {}".format(where_query))

                        else:

                            logging.info("Join is not empty")

                            if c_value.isnumeric():
                                c_value = int(c_value)
                                if c_operator == "LIKE" or c_operator == "NOT LIKE":
                                    where_query = where_query + " {} {} {}%  {}".format(c_name, c_operator, c_value,
                                                                                        c_join)
                                else:
                                    where_query = where_query + " {} {} {}  {}".format(c_name, c_operator, c_value,
                                                                                       c_join)
                                logging.info("query > {}".format(where_query))
                            else:
                                if c_operator == "LIKE" or c_operator == "NOT LIKE":
                                    where_query = where_query + " {} {} '{}%'  {}".format(c_name, c_operator, c_value,
                                                                                          c_join)
                                else:
                                    where_query = where_query + " {} {} '{}'  {}".format(c_name, c_operator,
                                                                                         c_value,
                                                                                         c_join)
                                logging.info("query > {}".format(where_query))

                ttr_check = "true"
                logging.info("where query > {}".format(where_query))
                rule_success = True

            else:
                logging.info("Rule {} and {} are out of range and is skipped".format(p_valid_from, p_valid_till))
                rule_success = False

        else:
            logging.info("Rule {} is not valid and is skipped".format(p_id))
            rule_success = False

        if rule_success:
            logging.info("Rule {} is success and the field name is {} ".format(p_id, p_field_name))
            if p_field_name == "lookup":
                check_rule = True
                check_rule_id = int(p_field_value)
                logging.info("check_rule_id is {} and check_rule is {} ".format(check_rule_id, check_rule))
            else:
                where_query = where_query + " order by id "
                total_query = select_query + where_query
                logging.info("total_query > {}".format(total_query))

    return total_query


def rule_generator():
    logging.info("Rule generator has been called")

    json_df = ingest_config()
    # logging.info(json_df.show(truncate=False))

    # collecting the dataframe back to the driver to pass it as a list for forming the query
    pdf, cdf = parse_json(json_df)
    # Show the schema of parent and child dataframe on console
    # logging.info(pdf.printSchema())
    # logging.info(cdf.printSchema())

    pdf_collect = pdf.collect()
    cdf_collect = cdf.collect()

    # Reading the source atm file and loading into a dataframe
    atm = ingest_atm_file()
    atm.createOrReplaceTempView("atm_transactions")

    # Pass the parent and child json dataframes to the rules_pipeline function to return the query
    total_query = rules_pipeline(pdf_collect, cdf_collect, "atm_transactions")

    return total_query


if __name__ == '__main__':
    logging.basicConfig(level="INFO")

    spark = SparkSession \
        .builder \
        .appName("Rules Engine") \
        .master("local[*]") \
        .enableHiveSupport() \
        .getOrCreate()

    logging.info("spark session created")

    logging.info("Call the rule generator")
    total_query = rule_generator()

    with open("output/queries.txt", "w") as f:
            f.write(total_query)
            f.write("\n\n")

    tempDf = spark.sql(total_query)
    logging.info(tempDf.show(truncate=False))
    # tempDf.repartition(1).write.option("header", "true").csv("output/Dataframe")


