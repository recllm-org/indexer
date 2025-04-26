from .utils import EnvVars
from google import genai
import cohere



class Client:
	@staticmethod
	def gemini():
		return genai.client.Client(api_key=EnvVars.get('GEMINI_API_KEY'))
	
	@staticmethod
	def cohere():
		return cohere.ClientV2(api_key=EnvVars.get('COHERE_API_KEY'))