import logging

class Logging:
  logger = None

  def __init__(self, subject):
    self.logger = logging.getLogger(subject)
    self.logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')	# 4-5
    stream_hander = logging.StreamHandler()
    stream_hander.setFormatter(formatter)
    self.logger.addHandler(stream_hander)
    file_handler = logging.FileHandler(f'../logs/{subject}.log')
    file_handler.setFormatter(formatter)
    self.logger.addHandler(file_handler)

  def getLogger(self):
    return self.logger