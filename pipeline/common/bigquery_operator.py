from google.cloud import bigquery
from datetime import datetime, timedelta
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file('./catcher-service-key.json')

class BigQueryClient:
  client = ''


  def __init__(self):
    self.client = bigquery.Client(credentials=credentials, project='aiffel-gn3-6')


  def insert_rows(self, table_name, data):
    table_id = 'aiffel-gn3-6.socar.' + table_name
    errors = self.client.insert_rows_json(table_id, data)
    if errors == []:
      print("New rows have been added >>>> ", data)
    else:
      print("Encountered errors while inserting rows: {}".format(errors))


  def select_all_keywords(self):
    table_id = 'aiffel-gn3-6.socar.keyword'
    result = self.client.insert_rows_json(table_id) # TODO
    print("rows have been selected >>>> ", len(result))
    return result

  
  def select_all_category(self):
    table_id = 'aiffel-gn3-6.socar.category'
    result = self.client.insert_rows_json(table_id) # TODO
    print("rows have been selected >>>> ", len(result))
    return result


  def select_keywords_for_rocket(self):
    end_at = datetime.today()
    days = timedelta(14)
    start_at = end_at - days

    sql = ("SELECT rk.keyword_id, ky.keyword, rv.create_at "
      "FROM aiffel-gn3-6.socar.review_keyword AS rk "
      "LEFT JOIN aiffel-gn3-6.socar.keyword AS ky on ky.keyword_id = rk.keyword_id "
      "LEFT JOIN aiffel-gn3-6.socar.review AS rv on rv.review_id = rk.review_id "
      "WHERE rv.create_at > cast('{0} 00:00:00' AS TIMESTAMP) "
      "AND rv.create_at <= cast('{1} 23:59:59' AS TIMESTAMP)"
      ).format(start_at.strftime('%Y-%m-%d'), end_at.strftime('%Y-%m-%d'))
    query_job = self.client.query(sql)

    results = query_job.result()
    return results


  def close(self):
    self.client.close()


# rows_to_insert = [
#     {"full_name": "Phred Phlyntstone", "age": 32},
#     {"full_name": "Wylma Phlyntstone", "age": 29},
# ]
# bigquery_client = BigQueryClient()
# bigquery_client.insert_rows('test', rows_to_insert)

bigquery_client = BigQueryClient()
bigquery_client.select_keywords_for_rocket()