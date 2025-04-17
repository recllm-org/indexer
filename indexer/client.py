from .utils import EnvVars
from google import genai
import os
import supabase



class Client:
	@staticmethod
	def supabase():
		client = supabase.create_client(EnvVars.get('SUPABASE_URL'), EnvVars.get('SUPABASE_KEY'))
		return client
	
	@staticmethod
	def gemini():
		return genai.client.Client(api_key=EnvVars.get('GEMINI_API_KEY'))