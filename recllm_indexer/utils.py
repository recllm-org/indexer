import base64
import requests



def get_tablename(SATable):
  if hasattr(SATable, '__table__'):
    return SATable.__table__.name
  elif hasattr(SATable, '__tablename__'):
    return SATable.__tablename__
  else:
    raise ValueError(f'Table {SATable} has no __table__ or __tablename__ attribute')


def imgurl2b64(img_url):
  """
  Converts an image url to a base64 encoded string
  """
  response = requests.get(img_url)
  response.raise_for_status()
  content_type = response.headers.get('content-type', '')
  if not content_type.startswith('image/'):
    raise ValueError(f'URL does not point to an image. Content-Type: {content_type}')
  img_format = content_type.split('/')[-1]
  img_b64 = base64.b64encode(response.content).decode('utf-8')
  img_b64 = f'data:image/{img_format};base64,{img_b64}'
  return img_b64


def imgpath2b64(img_path):
  """
  Converts an image path to a base64 encoded string
  """
  _, file_extension = os.path.splitext(img_path)
  img_format = file_extension[1:]
  with open(img_path, 'rb') as fp:
    img_b64 = base64.b64encode(fp.read()).decode('utf-8')
    img_b64 = f'data:image/{img_format};base64,{img_b64}'
  return img_b64


def construct_cohere_contents(contents):
  """
  Constructs cohere specific contents given input of the form:
    [
      [
        {'text': },
        {'image_url': },
        {'image_path':}
      ],
    ]
  """
  cohere_contents = []
  for content in contents:
    cohere_content = {'content': []}
    for actual_content in content:
      assert len(actual_content)==1, 'Only one type of content should be specified!'
      if actual_content.get('text'):
        cohere_content['content'].append({'type': 'text', 'text': actual_content['text']})
      elif actual_content.get('image_url'):
        cohere_content['content'].append({'type': 'image_url', 'image_url': {'url': imgurl2b64(actual_content['image_url'])}})
      elif actual_content.get('image_path'):
        cohere_content['content'].append({'type': 'image_url', 'image_url': {'url': imgpath2b64(actual_content['image_path'])}})
      else:
        raise ValueError('Invalid content type!')
    cohere_contents.append(cohere_content)
  return cohere_contents
