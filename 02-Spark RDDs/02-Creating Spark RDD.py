# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Read File")
sc = SparkContext.getOrCreate(conf=conf)
text = sc.textFile('/FileStore/tables/sample.txt')
print('\n\n\n')
print(text.collect())
print('\n\n\n')

