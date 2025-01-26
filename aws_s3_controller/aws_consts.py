from dotenv import load_dotenv
import os 

load_dotenv()

AWS_ACCESS_KEY_ID = str(os.getenv('AWS_ACCESS_KEY_ID'))
AWS_SECRET_ACCESS_KEY = str(os.getenv('AWS_SECRET_ACCESS_KEY'))
AWS_REGION = str(os.getenv('AWS_REGION'))