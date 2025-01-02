from hate.logger import logging
from hate.exception import CustomException
import sys

# logging.info("Welcome!")

try:
    a = 7 / "0"

except Exception as e:
    raise CustomException(e, sys)    