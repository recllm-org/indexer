from indexer import Indexer
from indexer.config import Config
import requests
import time
from dataclasses import dataclass
import xml.etree.ElementTree as ET

@dataclass
class Paper:
  id: str
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
        id=paper_id,
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

config = Config(
  user_tables={},
  item_tables={'Papers': papers}
)
indexer = Indexer(config)

def context_constructor(obj):
  return f'{obj.title}' # this maybe same as content or different according to requirements

def content_constructor(obj):
  return f'{obj.title}\n {obj.abstract}'

fetcher = ArxivFetcher()
papers = fetcher.fetch_papers(max_results=10)
indexer.index(papers, context_constructor, content_constructor)