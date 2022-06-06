import hashlib

def make_hash_id(data_str):
  result = hashlib.md5(data_str.encode())
  return result.hexdigest()