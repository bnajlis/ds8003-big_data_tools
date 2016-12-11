import sys, getopt
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, DataFrameWriter
from pyspark.sql.functions import *

# ########################################################################
# TF-IDF Index builder funtion
##########################################################################
# Input Parameters:
#		sc: Spark Context (created in main function)
#		sqlContext: SparkSql Context (created in main function)
#		docs_path: string with path containing documents to index
# Returns:
#		tf_idf: DataFrame containing TF-IDF index
#			- Schema for DataFrame:
#				- doc_name: string containing original document name path
#				- word: word
#				- tf_idf: TF-IDF score for the word in doc_name
#########################################################################
def build_tf_idf_index(sc, sqlContext, docs_path):
	
	# Load files into RDD
	textFiles = sc.wholeTextFiles(docs_path)
	# Count number of documents in total to use in IDF calculations
	num_docs = textFiles.count() # ACTION

	###### Build TF-IDF index ######

	# Split document into words (still in RDD)
	tmpFilesRDD = textFiles.map(lambda docs:(docs[0], docs[1].split(" "))) #transformation
	# Convert RDD into Data Frame, to use SparkSQL
	textFilesDF = tmpFilesRDD.toDF(["doc_name", "doc_content"]) # ACTION
	# Explode the Data Frame to get one row per document*word combination. This DF is the basis for TF and IDF
	words_by_docDF = textFilesDF.select(textFilesDF.doc_name, explode(textFilesDF.doc_content).alias("word")) #transformation

	### Build Term Frequency table ###

	# First count each word in all documents
	pre_tf = words_by_docDF.groupBy(words_by_docDF.doc_name, words_by_docDF.word).count() #transformation
	# Do sum() aggregation of counts and set as column 'tf'
	tf = pre_tf.groupBy(words_by_docDF.doc_name, words_by_docDF.word, ).agg(sum("count").alias("tf")) #transformation

	### Build Inverse Document Frequency table ###

	# First do a count distinc group by words, to get the number of docs using each word
	pre_idf = words_by_docDF.distinct().groupby(words_by_docDF.word).count() #transformation
	# Now calculate IDF as log(num_docs / docs_using_word)
	idf = pre_idf.select(pre_idf.word, col("count"), log10(num_docs/(col("count"))).alias("idf")) #transformation

	# Join TF with IDF dataframes (TF left outer join IDF on word) so we have both TF and IDF per word*document
	pre_tf_idf = tf.join(idf, tf.word == idf.word, 'outer') #transformation
	
	# Build TF-IDF by multiplying TF * IDF
	tf_idf = pre_tf_idf.select(col("doc_name"), tf["word"], (col("tf") * col("idf")).alias("tf_idf")) #transformation
	
	# Save index to HDFS parquet format
	tf_idf.write.save("spark-tfidf.parquet")
	
	return tf_idf

def match_words(sc,tfidf_index,all_words, ndocs):
	
	# words are submittes as comma separated list, so we split the string into a python list first
	all_words = words.split(',')
	# count how many words are in the query for the normalization factor
	query_words_qty = len(all_words)
	# then we convert the list into a spark data frame (passing through a parallelized rdd and dummy lambda function)
	words_df = sc.parallelize(all_words).map(lambda x:(x,)).toDF(["query_word"])
	# get a subset data frame with just the query words
	joined = tfidf_index.join(words_df, words_df.query_word == tfidf_index.word, "inner")
	# counts how many matched words and aggregates (sum) the score per word
	pre_scored = joined.groupBy("doc_name").agg({"*":"count", "tf_idf":"sum"}).withColumnRenamed("count(1)", "matched_words_qty").withColumnRenamed("sum(tf_idf)", "pre_score")
	# calculate the tf_idf score for the document as pre_score (summ of per-word score) times number of matched words divided by num of words in query
	pre_scored2 = pre_scored.select(pre_scored.doc_name, (pre_scored.pre_score * pre_scored.matched_words_qty) / query_words_qty)
	# rename the matching score calculated column
	scored = pre_scored2.withColumnRenamed("((pre_score * matched_words_qty) / " + str(query_words_qty) + ")", "matching_score")
	# sort the documents by descending matching score and return only the top ndocs requested
	doc_matches = scored.sort("matching_score", ascending=False).limit(int(ndocs))

	return doc_matches

def usage(err):
	# Prints error message and command line usage help
	print(err)
	print()
	print("Example: -w(word1,word2,word3) -n10")
	print()
	print("This will search for 'word1', 'word2' and 'word3' and return the top 10 article matches.")

# Main entry function
# Sets Spark and Sql Context to call Index builder and Document matching query 
def main(docs_path, words, ndocs):
	c = SparkConf().setAppName("TF-IDF Indexer") # Set Spark configuration
	sc = SparkContext(conf = c)	# Set spark context with config
	sc.setLogLevel("ERROR")
	sql = SQLContext(sc)

	tf_idf_index = build_tf_idf_index(sc, sql, docs_path)

	matches = match_words(sc,tf_idf_index, words, ndocs)

	matches.show(int(ndocs), False)

	sc.stop()					# Stop Spark context after done with main function


# Catch all to parse command line options and redirect to main function
if __name__ == "__main__":
	try:
		# Gets command line options required for script usage
		opts, args = getopt.getopt(sys.argv[1:],"d:w:n:")
	except getopt.GetoptError as err:
		usage(err);
		sys.exit()

	
	if(len(opts) < 2 ):
		usage("Error: Command line parameters not specified.")
		sys.exit()

	# Gets command line parameters
	for opt,arg in opts:
		if opt == "-d":
			docs_path = arg # Path containing documents to be indexed
		if opt == "-w":
			words = arg		# List of words to use for query
		if opt == "-n":		
			ndocs = arg		# Number of documents to return in matching query

	main(docs_path, words, ndocs)