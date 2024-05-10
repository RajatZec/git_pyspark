import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField, StringType ,ArrayType

# Create a Spark session
spark = SparkSession.builder.appName("task1").getOrCreate()


from pyspark.sql.functions import col, explode
from pyspark.sql.functions import concat_ws


def explode_arrays(df):
    """
    Explode array columns into separate rows.
    """
    array_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, ArrayType)]
    for column in array_columns:
        df = df.withColumn(column, F.explode_outer(column))
    return df

def flatten_df(df):
    """
    Flatten DataFrame with nested structures.
    """
    # Explode array columns
    df_exploded = df.select("*", explode("additional_data.additionalInfo.Payments").alias("exploded_Payments"))
    df_exploded = df_exploded.select("*", explode("additional_data.additionalInfo.Service options").alias("exploded_Service_options"))
    df_exploded = df_exploded.select("*", explode("additional_data.peopleAlsoSearch").alias("exploded_peopleAlsoSearch"))
    df_exploded = df_exploded.select("*", explode("additional_data.questionsAndAnswers").alias("exploded_questionsAndAnswers"))
    df_exploded = df_exploded.select("*", explode("additional_data.reviewsTags").alias("exploded_reviewsTags"))
    
    # Select required columns and flatten struct fields
    df_flattened = df_exploded.select(
        concat_ws(",", col("additional_data.additionalInfo.Payments.Credit cards")).alias("Credit_cards"),
        col("additional_data.additionalInfo.Service options.Delivery").alias("Delivery"),
        col("additional_data.categories").alias("categories"),
        col("additional_data.imageUrls").alias("imageUrls"),
        col("additional_data.mapped_category.Shopping").alias("Shopping"),
        col("additional_data.openingHours").alias("openingHours"),
        col("additional_data.orderBy").alias("orderBy"),
        col("exploded_peopleAlsoSearch.category").alias("peopleAlsoSearch_category"),
        col("exploded_peopleAlsoSearch.reviewsCount").alias("peopleAlsoSearch_reviewsCount"),
        col("exploded_peopleAlsoSearch.title").alias("peopleAlsoSearch_title"),
        col("exploded_peopleAlsoSearch.totalScore").alias("peopleAlsoSearch_totalScore"),
        col("additional_data.placesTags").alias("placesTags"),
        col("additional_data.popularTimesHistogram").alias("popularTimesHistogram"),
        col("additional_data.popularTimesLivePercent").alias("popularTimesLivePercent"),
        col("additional_data.popularTimesLiveText").alias("popularTimesLiveText"),
        col("exploded_questionsAndAnswers.answers.answer").alias("questionsAndAnswers_answer"),
        col("exploded_questionsAndAnswers.answers.answerDate").alias("questionsAndAnswers_answerDate"),
        col("exploded_questionsAndAnswers.answers.answeredBy.name").alias("questionsAndAnswers_answeredBy_name"),
        col("exploded_questionsAndAnswers.answers.answeredBy.url").alias("questionsAndAnswers_answeredBy_url"),
        col("exploded_questionsAndAnswers.askDate").alias("questionsAndAnswers_askDate"),
        col("exploded_questionsAndAnswers.askedBy.name").alias("questionsAndAnswers_askedBy_name"),
        col("exploded_questionsAndAnswers.askedBy.url").alias("questionsAndAnswers_askedBy_url"),
        col("exploded_questionsAndAnswers.question").alias("questionsAndAnswers_question"),
        col("additional_data.reviewsDistribution.fiveStar").alias("reviewsDistribution_fiveStar"),
        col("additional_data.reviewsDistribution.fourStar").alias("reviewsDistribution_fourStar"),
        col("additional_data.reviewsDistribution.oneStar").alias("reviewsDistribution_oneStar"),
        col("additional_data.reviewsDistribution.threeStar").alias("reviewsDistribution_threeStar"),
        col("additional_data.reviewsDistribution.twoStar").alias("reviewsDistribution_twoStar"),
        col("exploded_reviewsTags.count").alias("reviewsTags_count"),
        col("exploded_reviewsTags.title").alias("reviewsTags_title"),
        col("additional_data.similarHotelsNearby").alias("similarHotelsNearby"),
        col("additional_data.subTitle").alias("subTitle"),
        col("additional_data.updatesFromCustomers").alias("updatesFromCustomers"),
        col("address").alias("address"),
        col("categoryName").alias("categoryName"),
        col("cid").alias("cid"),
        col("city").alias("city"),
        col("claimThisBusiness").alias("claimThisBusiness"),
        col("countryCode").alias("countryCode"),
        col("depth").alias("depth"),
        col("description").alias("description"),
        col("discords").alias("discords"),
        col("domain").alias("domain"),
        col("emails").alias("emails"),
        col("facebooks").alias("facebooks"),
        col("googleFoodUrl").alias("googleFoodUrl"),
        col("hotelDescription").alias("hotelDescription"),
        col("hotelStars").alias("hotelStars"),
        col("imagesCount").alias("imagesCount"),
        col("instagrams").alias("instagrams"),
        col("isAdvertisement").alias("isAdvertisement"),
        col("linkedIns").alias("linkedIns"),
        col("locatedIn").alias("locatedIn"),
        col("location.lat").alias("location_lat"),
        col("location.lng").alias("location_lng"),
        col("menu").alias("menu"),
        col("name").alias("name"),
        col("neighborhood").alias("neighborhood"),
        col("originalStartUrl").alias("originalStartUrl"),
        col("permanentlyClosed").alias("permanentlyClosed"),
        col("phone").alias("phone"),
        col("phoneUnformatted").alias("phoneUnformatted"),
        col("phones").alias("phones"),
        col("phonesUncertain").alias("phonesUncertain"),
        col("pinterests").alias("pinterests"),
        col("placeId").alias("placeId"),
        col("plusCode").alias("plusCode"),
        col("postalCode").alias("postalCode"),
        col("price").alias("price"),
        col("ranking").alias("ranking"),
        col("referrerUrl").alias("referrerUrl"),
        col("reserveTableUrl").alias("reserveTableUrl"),
        col("reviews").alias("reviews"),
        col("reviewsCount").alias("reviewsCount"),
        col("searchPageLoadedUrl").alias("searchPageLoadedUrl"),
        col("searchPageUrl").alias("searchPageUrl"),
        col("searchString").alias("searchString"),
        col("state").alias("state"),
        col("street").alias("street"),
        col("street_name").alias("street_name"),
        col("tiktoks").alias("tiktoks"),
        col("totalScore").alias("totalScore"),
        col("twitters").alias("twitters"),
        col("url").alias("url"),
        col("website").alias("website"),
        col("youtubes").alias("youtubes")
    )
    
    return df_flattened


def read_json_file(file_path):
    """
    Read JSON data from a file and return as a DataFrame, handling corrupt records.
    """
    print("file path", file_path)




    spark = SparkSession.builder.appName("task1").config("spark.executor.instances", "4").config("spark.executor.cores", "4").config("spark.executor.memory", "16g").getOrCreate()

    # Define the schema
    schema = StructType([
        StructField("additional_data", StructType([
            StructField("additionalInfo", StructType([
                StructField("Payments", ArrayType(StructType([
                    StructField("Credit cards", StringType(), True)
                ])), True),
                StructField("Service options", ArrayType(StructType([
                    StructField("Delivery", StringType(), True)
                ])), True)
            ]), True),
            StructField("categories", ArrayType(StringType(), True), True),
            StructField("imageUrls", ArrayType(StringType(), True), True),
            StructField("mapped_category", StructType([
                StructField("Shopping", ArrayType(StringType(), True), True)
            ]), True),
            StructField("openingHours", ArrayType(StringType(), True), True),
            StructField("orderBy", ArrayType(StringType(), True), True),
            StructField("peopleAlsoSearch", ArrayType(StructType([
                StructField("category", StringType(), True),
                StructField("reviewsCount", StringType(), True),
                StructField("title", StringType(), True),
                StructField("totalScore", StringType(), True)
            ])), True),
            StructField("placesTags", ArrayType(StringType(), True), True),
            StructField("popularTimesHistogram", StringType(), True),
            StructField("popularTimesLivePercent", StringType(), True),
            StructField("popularTimesLiveText", StringType(), True),
            StructField("questionsAndAnswers", ArrayType(StructType([
                StructField("answers", ArrayType(StructType([
                    StructField("answer", StringType(), True),
                    StructField("answerDate", StringType(), True),
                    StructField("answeredBy", StructType([
                        StructField("name", StringType(), True),
                        StructField("url", StringType(), True)
                    ]), True)
                ])), True),
                StructField("askDate", StringType(), True),
                StructField("askedBy", StructType([
                    StructField("name", StringType(), True),
                    StructField("url", StringType(), True)
                ]), True),
                StructField("question", StringType(), True)
            ])), True),
            StructField("reviewsDistribution", StructType([
                StructField("fiveStar", StringType(), True),
                StructField("fourStar", StringType(), True),
                StructField("oneStar", StringType(), True),
                StructField("threeStar", StringType(), True),
                StructField("twoStar", StringType(), True)
            ]), True),
            StructField("reviewsTags", ArrayType(StructType([
                StructField("count", StringType(), True),
                StructField("title", StringType(), True)
            ])), True),
            StructField("similarHotelsNearby", StringType(), True),
            StructField("subTitle", StringType(), True),
            StructField("updatesFromCustomers", StringType(), True)
        ]), True),
        StructField("address", StringType(), True),
        StructField("categoryName", StringType(), True),
        StructField("cid", StringType(), True),
        StructField("city", StringType(), True),
        StructField("claimThisBusiness", StringType(), True),
        StructField("countryCode", StringType(), True),
        StructField("depth", StringType(), True),
        StructField("description", StringType(), True),
        StructField("discords", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("emails", StringType(), True),
        StructField("facebooks", StringType(), True),
        StructField("googleFoodUrl", StringType(), True),
        StructField("hotelDescription", StringType(), True),
        StructField("hotelStars", StringType(), True),
        StructField("imagesCount", StringType(), True),
        StructField("instagrams", StringType(), True),
        StructField("isAdvertisement", StringType(), True),
        StructField("linkedIns", StringType(), True),
        StructField("locatedIn", StringType(), True),
        StructField("location", StructType([
            StructField("lat", StringType(), True),
            StructField("lng", StringType(), True)
        ]), True),
        StructField("menu", StringType(), True),
        StructField("name", StringType(), True),
        StructField("neighborhood", StringType(), True),
        StructField("originalStartUrl", StringType(), True),
        StructField("permanentlyClosed", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("phoneUnformatted", StringType(), True),
        StructField("phones", StringType(), True),
        StructField("phonesUncertain", StringType(), True),
        StructField("pinterests", StringType(), True),
        StructField("placeId", StringType(), True),
        StructField("plusCode", StringType(), True),
        StructField("postalCode", StringType(), True),
        StructField("price", StringType(), True),
        StructField("ranking", StringType(), True),
        StructField("referrerUrl", StringType(), True),
        StructField("reserveTableUrl", StringType(), True),
        StructField("reviews", ArrayType(StringType(), True), True),
        StructField("reviewsCount", StringType(), True),
        StructField("searchPageLoadedUrl", StringType(), True),
        StructField("searchPageUrl", StringType(), True),
        StructField("searchString", StringType(), True),
        StructField("state", StringType(), True),
        StructField("street", StringType(), True),
        StructField("street_name", ArrayType(StringType(), True), True),
        StructField("tiktoks", StringType(), True),
        StructField("totalScore", StringType(), True),
        StructField("twitters", StringType(), True),
        StructField("url", StringType(), True),
        StructField("website", StringType(), True),
        StructField("youtubes", StringType(), True)
    ])

    # Read JSON file and create DataFrame
    # df = spark.read.schema(schema).option("multiline", "true").json("/home/ubuntu/Downloads/learn_pyspark/git_pyspark/test.json")
    df = spark.read.schema(schema).option("multiline", "true").json(file_path)
    df = df.repartition(2500)
    # Show DataFrame schema
    df.printSchema()
    no_of_partition = df.rdd.getNumPartitions()
    print("---->",no_of_partition)
    # df = spark.read.schema(schema).option("multiline", "true").json("/home/ubuntu/Downloads/learn_pyspark/git_pyspark/test.json")
    # df = spark.read.option("multiline", "true").json("/home/ubuntu/Downloads/learn_pyspark/git_pyspark/test.json")

    # df.show(5, truncate=False)
    return df
    
def write_to_postgres(df):
    # Write DataFrame to PostgreSQL
    df_converted = df.select([F.col(col_name).cast("string") if isinstance(df.schema[col_name].dataType, ArrayType) else F.col(col_name) for col_name in df.columns])
    df_converted.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/demo") \
        .option("dbtable", "abc") \
        .option("user", "rooot") \
        .option("password", "rooot") \
        .option("driver", "com.mysql.jdbc.Driver")\
        .mode("overwrite") \
        .save()
    
def process_json_files(local_dir):
    """
    Process JSON files from local_dir folder.
    """


    json_files = [os.path.join(local_dir, f) for f in os.listdir(local_dir) if f.endswith('.json')]
    for json_file in json_files:
        df = read_json_file(json_file)
        output_path = "/home/ubuntu/Downloads/learn_pyspark/git_pyspark/output"
        df_flattened = flatten_df(df)
        df_exploded = explode_arrays(df_flattened)
        df_exploded =  df_exploded.repartition(250)
        write_to_postgres(df_exploded)
        # df_flattened.write.parquet(output_path, mode="overwrite")
        write_to_postgres(df_flattened)



def main():
    local_dir = '/home/ubuntu/Downloads/velocity_json_data'  
    process_json_files(local_dir)

if __name__ == "__main__":
    main()
