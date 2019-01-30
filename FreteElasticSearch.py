from datetime import datetime, timedelta 
from pyspark.sql import SQLContext
from elasticsearch import Elasticsearch
from pandasticsearch import Select
from pyspark.sql.types import *
import shutil

# estrutura de armazanemento do DataFrame
schema = StructType([
    StructField("dateIndex", StringType(), True),
    StructField("partnerId", StringType(), True),
    StructField("skuB2W", StringType(), True),
    StructField("strategy", StringType(), True),
    StructField("originalStrategy", StringType(), True),
    StructField("itemId", StringType(), True),
    StructField("height", StringType(), True),
    StructField("length", StringType(), True),
    StructField("weight", StringType(), True),
    StructField("width", StringType(), True),
    StructField("destinationZipCode", StringType(), True),
    StructField("priceItem", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("finalShippingPrice", StringType(), True),
    StructField("ShippigCost", StringType(), True),
    StructField("deliveryTime", StringType(), True),
    StructField("extraDeliveryTime", StringType(), True)    
])

# cria contexto para o DataFrame
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

# -----------------------------------------------------------------------------
# retorna o nome do indice do dia anterior
# -----------------------------------------------------------------------------
def f_ReturnNameIndex():
    today = datetime.today()
    prior_date = today - timedelta(days=1)
    return datetime.strftime(prior_date, "frete-venda-%Y.%m.%d")


# -----------------------------------------------------------------------------
# faz o tratamento da paginação obtendo somente as informações desejadas
# grava o resultado em arquivo parquet
# -----------------------------------------------------------------------------
def f_build_shipping(esDict):
    df_elastic = Select.from_dict(esDict).to_pandas()
    rows_list=[]
    for index, row in df_elastic.iterrows():
        RecordtoAdd={} 
        RecordtoAdd.update({'dateIndex' : _NAME_INDEX}) 
        RecordtoAdd.update({'partnerId' : row['delivery']['partnerId']}) 
        RecordtoAdd.update({'skuB2W' : row['skuB2W'][0]['sku']})     
        RecordtoAdd.update({'strategy' : row['strategy']}) 
        RecordtoAdd.update({'originalStrategy' : row['originalStrategy']}) 
        RecordtoAdd.update({'itemId' : row['delivery']['items'][0]['itemId']}) 
        RecordtoAdd.update({'height' : row['delivery']['items'][0]['height']}) 
        RecordtoAdd.update({'length' : row['delivery']['items'][0]['length']})    
        RecordtoAdd.update({'weight' : row['delivery']['items'][0]['weight']})   
        RecordtoAdd.update({'width' : row['delivery']['items'][0]['width']})  
        RecordtoAdd.update({'destinationZipCode' : row['delivery']['destinationZipCode']})
        RecordtoAdd.update({'priceItem' : row['delivery']['items'][0]['price']}) 
        RecordtoAdd.update({'quantity' : row['delivery']['items'][0]['quantity']}) 
        if  f_check_value_shipping_quote(row['shippingEstimate']): 
            RecordtoAdd.update({'finalShippingPrice' : row['shippingEstimate']['quotes'][0]['finalShippingPrice']})
            RecordtoAdd.update({'shippingCost' : row['shippingEstimate']['quotes'][0]['shippingCost']})
            RecordtoAdd.update({'deliveryTime' : row['shippingEstimate']['quotes'][0]['deliveryTime']})    
            RecordtoAdd.update({'extraDeliveryTime' : row['shippingEstimate']['quotes'][0]['extraDeliveryTime']})
        else: 
            RecordtoAdd.update({'finalShippingPrice' : None})
            RecordtoAdd.update({'shippingCost' : None})
            RecordtoAdd.update({'deliveryTime' : None})    
            RecordtoAdd.update({'extraDeliveryTime' : None})
        rows_list.append(RecordtoAdd)
    df1 = sqlContext.createDataFrame(rows_list, schema).repartition(1)
    df1.write.format('com.databricks.spark.csv').options(header='true').save(path="c:/temp/"+_NAME_INDEX,mode="append") 
    #df1.write.save(path="c:/temp/"+_NAME_INDEX,format="parquet",mode="append")

    
# -----------------------------------------------------------------------------
# Verifica se temos valor valido na informações de shippingEstimate
# -----------------------------------------------------------------------------
def f_check_value_shipping_quote(p_row):
    vReturn = True
    try:
        if p_row is None:
            return False
        if p_row is not None and len(p_row) > 0:
            if p_row['quotes'] is not None:
                if len(p_row['quotes']) > 0:
                    vReturn = True
                else:
                    vReturn = False
            else:
                vReturn = False     
        else:
            vReturn = False    
    except:
        vReturn = False
    return vReturn   

def f_execute_search_index():
    es = Elasticsearch('http://10.239.20.13:9200')
    # scan inicial para obter a quantidade de linhas
    page = es.search(
              index = _NAME_INDEX,
              scroll = '2m',
              search_type = 'scan',
              size = 2000,
              body =  {"_source": ["shippingEstimate","delivery","status","strategy","skuB2W","originalStrategy"],"query": {"match_all": {}}}
              #  body =  {"query": {"match_all": {}}}
            )

    # tamanho total do indice
    sid = page['_scroll_id']             #scroll inicial
    scroll_size = page['hits']['total']  

    # Start scrolling
    while (scroll_size > 0):
        page = es.scroll(scroll_id = sid, scroll = '2m')
        # Update the scroll ID
        sid = page['_scroll_id']
        scroll_size = len(page['hits']['hits'])
        if sid:
            f_build_shipping(page)

if __name__ == '__main__':
    _NAME_INDEX = f_ReturnNameIndex()
    # apaga arquivo caso existente
    shutil.rmtree('/temp/'+_NAME_INDEX, ignore_errors=True)
    # sapeca o iaiaaaaa
    f_execute_search_index()