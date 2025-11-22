import logging

class Logger:
    def __init__(self, file_name: str = None) -> None:
        self.logger = logging.getLogger(file_name if file_name else __name__)
        self.logger.setLevel(logging.INFO)
        self.setup(file_name if file_name else 'app_logger')
    
    def setup(self, file_name: str) -> None:
        self.file_handler = logging.FileHandler(file_name + '.log')
        self.console_handler = logging.StreamHandler()
        self.formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.file_handler.setFormatter(self.formatter)
        self.console_handler.setFormatter(self.formatter)
        self.logger.addHandler(self.file_handler)
        self.logger.addHandler(self.console_handler)

    def log(self, message: str) -> None:
        self.logger.info(message)

    def info(self, message: str) -> None:
        self.logger.info(message)
        