from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

print('Starting Spark Session..')

spark = SparkSession.builder \
        .appName('CrimeFlowPipeline') \
        .config('spark.eventLog.enabled','false') \
        .getOrCreate()
print('Spark Session Created Successfully')

#CONFIG
PROJECT_ID = 'KeystoneProject'
DATASET = 'BankingCrimeFlow'
BUCKET = 'keystone_26'

#GCS FILE PATHS
txn_path = ('gs://keystone_26/Raw/Datafiles_transactions_raw.csv')
cust_kyc_path = ('gs://keystone_26/Raw/Datafiles_customers_kyc.json')
branch_path = ('gs://keystone_26/Raw/Datafiles_branch_lookup.csv')

#READ TRANSACTION FILE
print('Reading Transaction File..')
txn_df = spark.read \
        .option('header','true') \
        .option('inferschema','true') \
        .csv(txn_path)

print('TRANSACTION FILE LOADED')
txn_df.show(5)

print('Reading customer File..')
customer_df = spark.read \
        .option('header','true')\
        .option('inferschema','true')\
        .option('multiLine','true')\
        .json(cust_kyc_path)

print('TRANSACTION FILE LOADED')
customer_df.show(5)


print('Reading Branch File..')
branch_df = spark.read \
        .option('header','true')\
        .option('inferschema','true')\
        .option('multiLine','true')\
        .csv(branch_path)

print('TRANSACTION FILE LOADED')
branch_df.show(5)

#valid_records
print('Valid Dataframe..No Null Customer_IDs')
valid_df = txn_df.filter(F.col('customer_id').isNotNull())
valid_df.show(5)
print()

#invalid_records
print('invalid Dataframe..with Null IDs')
invalid_df = txn_df.filter(F.col('customer_id').isNull())\
                   .withColumn('failure_reason',F.lit('Null_Customer_ID'))
invalid_df.show(5)

# clean amount

print('clean amount.. without Invalid symbols')
clean_amount_df = valid_df.withColumn('amount',F.regexp_replace('amount','£|,',''))
clean_amount_df.show(5)

print('Changing amount column to numeric')
clean_df = clean_amount_df.withColumn('amount',F.col('amount').cast('double'))
clean_amount_df.show(5)

print('Postive values only')
clean_df = clean_df.withColumn('amount_gbp',F.abs(F.col('amount')))
clean_df.show(5)

print('Is_refund column..')
clean_df=clean_df.withColumn('is_refund',F.when(F.col('amount') <0, True).otherwise (False))
clean_df.show(5)

clean_df.filter(F.col('is_refund')== 'TRUE').show()

print('Currency unit GBP Standardised and filled for Null rows')
clean_df = clean_df.withColumn('currency_unit',F.when(F.trim(F.upper(F.col('currency_unit'))) != 'GBP', 'GBP').otherwise(F.trim(F.upper(F.col('currency_unit')))))
clean_df.show(5)

print('Channel column Values Changed to Capital')
clean_df = clean_df.withColumn('channel',(F.trim(F.upper(F.col('channel')))))
clean_df.show(10)

print('Date Cleansing Standardized to single format')
clean_df = clean_df.withColumn('transaction_date', F.coalesce(F.to_date(F.col('transaction_date'),'dd/MM/yyyy'), F.to_date(F.col('transaction_date'),'yyyy-MM-dd')))
clean_df.show(10)

print('Duplicated Filtered')
window_spec = Window.partitionBy('transaction_id').orderBy(F.col('transaction_date'))
clean_df = clean_df.withColumn('row_num',F.row_number().over(window_spec))
clean_df.filter(F.col('row_num') > 1).show()

print('Duplicates removed')
clean_df = clean_df.filter(F.col('row_num')==1).drop(F.col('row_num'))
clean_df.show()

print('customer_df tranformation')
customer_df = customer_df.withColumn('pep_flag',F.coalesce(F.col('pep_flag'),F.lit(False)))
customer_df = customer_df.withColumn('risk_rating', F.initcap(F.col('risk_rating')))
customer_df.show()

print('final_df')
final_df = clean_df.join(customer_df.select('customer_id','full_name','risk_rating','pep_flag'),on='customer_id',how = 'left')
final_df.show()

print('add is_pep_transaction')
final_df = final_df.withColumn('is_pep_transaction',F.when(F.col('pep_flag')==True,True).otherwise(False))
display(final_df.limit(5).toPandas())

#Window Function
print('Total Transaction count shown')
cust_window = Window.partitionBy('customer_id')
final_df = final_df.withColumn('trascn_count',F.count('transaction_id').over(cust_window ))
final_df.show(5)

print('Total Amount shown')
final_df = final_df.withColumn('total_amount',F.round(F.sum(F.col('amount')).over(cust_window),2))
final_df.show(5)
display(final_df.limit(5).toPandas())

print('velocity_band shown')
final_df = final_df.withColumn('velocity_band',F.when(
                                                       (F.col('trascn_count')>10) |
                                                       (F.col('total_amount')>50000),'High').otherwise('Low'))
display(final_df.limit(10).toPandas())

print('final df join with Branch df')
#Branch lookup join Df
final_df = final_df.join(branch_df,on = 'branch_code', how = 'left')
display(final_df.limit(5).toPandas())

write cleansed parquet
print('parquet file written')
final_df.write.mode('overwrite')\
.parquet('gs://keystone_26/cleansed/final_transactions')

print('Reading Parqeut file')
parquet_df =  spark.read\
.parquet('gs://keystone_26/cleansed/final_transactions')
display(parquet_df.limit(5).toPandas())

print('Reading count')
record_count = parquet_df.count()
print(f'Total records :{record_count}')

count_df = spark.createDataFrame([(record_count,)],['record_count'])

count_df.write.mode(overwrite)\
                  .option(header,'True')\
                  .csv(f":gs://keystone_26/audit/")
cnt_df = spark.read.mode\
                  .option(header,'True')\
                  .option(inferschema,'True')\
                  .csv(f":gs://keystone_26/audit/")
cnt_df.show()
