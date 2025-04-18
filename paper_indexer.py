from indexer import Client, ItemTable, Function, Indexer
from indexer.embed import GeminiEmbedder, CohereEmbedder
from indexer.function import ContentEmbedder
from indexer.utils import img2b64, construct_cohere_contents
from dataclasses import dataclass
from google.genai.types import GenerateContentConfig
from sqlalchemy import text
import requests
import time
import xml.etree.ElementTree as ET

@dataclass
class Paper:
  arxiv_id: str
  title: str
  authors: list
  abstract: str
  submitted_date: str
  categories: list

class ArxivFetcher:
  ARXIV_API_URL = 'https://export.arxiv.org/api/query'
  MAX_RETRIES = 10
  RETRY_DELAY = 1.0  # 1 second delay between retries
  ARXIV_NS = {'atom': 'http://www.w3.org/2005/Atom'}

  def __init__(self):
    self.start_index = 0

  @staticmethod
  def parse_papers(xml_data):
    root = ET.fromstring(xml_data)
    entries = root.findall('atom:entry', ArxivFetcher.ARXIV_NS)
    if not entries:
      return []

    papers = []
    for entry in entries:
      # Get primary category
      primary_category = entry.find('atom:primary_category', ArxivFetcher.ARXIV_NS)
      primary_cat = primary_category.get('term') if primary_category is not None else ''
      
      # Get additional categories
      categories = entry.findall('atom:category', ArxivFetcher.ARXIV_NS)
      additional_cats = [cat.get('term') for cat in categories if cat.get('term')]

      # Filter out duplicates and ensure primary category is first
      all_categories = [primary_cat] + additional_cats
      all_categories = list(dict.fromkeys(cat for cat in all_categories if cat))

      # Handle authors
      authors = entry.findall('atom:author/atom:name', ArxivFetcher.ARXIV_NS)
      author_names = [author.text for author in authors if author.text]

      # Get ID and clean it
      id_elem = entry.find('atom:id', ArxivFetcher.ARXIV_NS)
      paper_id = id_elem.text.split('/')[-1] if id_elem is not None else ''

      # Get title and clean it
      title_elem = entry.find('atom:title', ArxivFetcher.ARXIV_NS)
      title = ' '.join(title_elem.text.split()) if title_elem is not None else ''

      # Get abstract and clean it
      summary_elem = entry.find('atom:summary', ArxivFetcher.ARXIV_NS)
      abstract = ' '.join(summary_elem.text.split()) if summary_elem is not None else ''

      # Get published date
      published_elem = entry.find('atom:published', ArxivFetcher.ARXIV_NS)
      published = published_elem.text if published_elem is not None else ''

      paper = Paper(
        arxiv_id=paper_id,
        title=title,
        authors=author_names,
        abstract=abstract,
        submitted_date=published,
        categories=all_categories
      )
      papers.append(paper)

    return papers

  def fetch_papers(
    self,
    max_results=10,
    retry_count=0,
    selected_categories=None
  ):
    try:
      if selected_categories and len(selected_categories) > 0:
        search_query = ' OR '.join(f'cat:{cat}' for cat in selected_categories)
      else:
        search_query = 'cat:cs.AI'

      params = {
        'search_query': search_query,
        'start': self.start_index,
        'max_results': max_results,
        'sortBy': 'submittedDate',
        'sortOrder': 'descending'
      }

      response = requests.get(self.ARXIV_API_URL, params=params)
      response.raise_for_status()
      
      papers = self.parse_papers(response.text)
      
      if not papers:
        raise ValueError('No papers returned')

      # Update start_index for next fetch
      self.start_index += len(papers)
      return papers

    except Exception as err:
      if retry_count < self.MAX_RETRIES:
        time.sleep(self.RETRY_DELAY)
        return self.fetch_papers(
          max_results=max_results,
          retry_count=retry_count + 1,
          selected_categories=selected_categories
        )
      raise err


class PaperContentContext(Function):
  def fn(self, row, gemini_client=Client.gemini()):
    # content
    content = f'{row.title}\n{row.abstract}'
    row.cache.content = content
    # context
    chat = gemini_client.chats.create(
      model='gemini-2.0-flash-lite',
      config=GenerateContentConfig(
        system_instruction='''Generate a super concise TL;DR (max 5 sentences) of the paper based on the title and abstract.
        It need not make grammatical sense, but should be scientifically accurate.
        Output should only contain the contents of the TL;DR, no other text. Dont mention TL;DR in the output.
        '''
      )
    )
    response = chat.send_message(f'{row.title}\n {row.abstract}')
    context = response.text
    row.cache.context = context


indexer = Indexer([
  ItemTable(
    tablename='papers',
    classname='Papers',
    tracked_columns=['title', 'abstract'],
    functions=[
      PaperContentContext(),
      ContentEmbedder(
        GeminiEmbedder(task='RETRIEVAL_DOCUMENT'),
        # CohereEmbedder(task='search_document')
      )
    ]
  )
])
# indexer.update_stales()

fetcher = ArxivFetcher()
NUM_FETCHES = 1
MAX_PER_FETCH = 1
for _ in range(NUM_FETCHES):
  papers = fetcher.fetch_papers(max_results=MAX_PER_FETCH)
  with indexer.db.Session() as session:
    session.execute(text('SET session_replication_role = replica'))
    rows = []
    for paper in papers:
      rows.append(indexer.db.Papers(
        arxiv_id=paper.arxiv_id,
        title=paper.title,
        authors=paper.authors,
        abstract=paper.abstract,
        submitted_date=paper.submitted_date,
        categories=paper.categories
      ))
    session.add_all(rows)
    session.commit()
    indexer.index(rows)


# embedder = CohereEmbedder(task='search_document')
# embeddings = embedder.embed(
#   contents=construct_cohere_contents([
#     [
#       {'text': 'Hello, world!'},
#       {'image': '/Users/pranavsastry/Downloads/apod_tok.png'},
#       {'text': 'Hello again, world!'}
#     ],
#     [
#       {'text': 'Hello, world!'}
#     ]
#   ])
# )
# print(embeddings)
# print(len(embeddings))