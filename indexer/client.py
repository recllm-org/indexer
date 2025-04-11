from dotenv import load_dotenv
from google import genai
import os
import supabase



class Client:
	envars = dotenv_values('.env')

	@staticmethod
	def supabase():
		return supabase.create_client(Client.envars['SUPABASE_URL'], Client.envars['SUPABASE_KEY'])
	
	@staticmethod
	def gemini():
		return genai.Client(Client.envars['GEMINI_API_KEY'])