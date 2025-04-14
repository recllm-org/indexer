from dotenv import dotenv_values
from google import genai
import os
import supabase



class Client:
	envars = dotenv_values('.env')

	@staticmethod
	def supabase():
		client = supabase.create_client(Client.envars.get('SUPABASE_URL'), Client.envars.get('SUPABASE_KEY'))
		return client
	
	@staticmethod
	def gemini():
		return genai.client.Client(api_key=Client.envars.get('GEMINI_API_KEY'))