from google.cloud import bigquery

class BigQueryClient:
  client = ''
  table_id = ''

  def __init__(self, table_name):
    self.client = bigquery.Client()
    self.table_id = 'aiffel-gn3-6.socar.' + table_name
  
  def insert_rows(self, data):
    errors = self.client.insert_rows_json(self.table_id, data)
    if errors == []:
        print("New rows have been added >>>> ", data)
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

  def close(self):
    self.client.close()


# rows_to_insert = [
#     {"full_name": "Phred Phlyntstone", "age": 32},
#     {"full_name": "Wylma Phlyntstone", "age": 29},
# ]
# bigquery_client = BigQueryClient(test)
# bigquery_client.insert_rows(rows_to_insert)