from bigquery_operator import BigQueryClient

bigquery_client = BigQueryClient()


def select_keyword_rocket_data():
  try:
    return bigquery_client.select_keywords_for_rocket()
  finally:
    bigquery_client.close()


def select_keyword_all():
  try:
    return bigquery_client.select_all_keywords()
  finally:
    bigquery_client.close()
  

def select_category_all():
  try:
    return bigquery_client.select_all_category()
  finally:
    bigquery_client.close()


def insert_data_to_BigQuery(table_name, data):
  try:
    bigquery_client.insert_rows(table_name, data)
  finally:
    bigquery_client.close()