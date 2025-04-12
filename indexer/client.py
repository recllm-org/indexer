from dotenv import load_dotenv
from google import genai
import os
import supabase



class Client:
	envars = dotenv_values('.env')

	@staticmethod
	def supabase():
		client = supabase.create_client(Client.envars.get('SUPABASE_URL'), Client.envars.get('SUPABASE_KEY'))
		client.rpc('sql', 'CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA extensions;')
		return client
	
	@staticmethod
	def gemini():
		return genai.Client(Client.envars.get('GEMINI_API_KEY'))