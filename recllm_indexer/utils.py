from dotenv import dotenv_values
import os
import base64



class EnvVars:
  """
  Environment variables
    - `envars` is a dictionary of environment variables
    - `get` is a method to get an environment variable
  
  `get`
    - If `include_os` is `True`, the environment variable is searched in both `os.environ` and `envars`
    - If `include_os` is `False`, the environment variable is only searched in `envars`
  """

  envars = dotenv_values('.env')
  
  @staticmethod
  def get(key, include_os=True): # include_os is useful if there are namespace conflicts, ie same key in .env and os.environ
    if include_os:
      return EnvVars.envars.get(key) or os.environ.get(key)
    else:
      return EnvVars.envars.get(key)


class Config:
  """
  Config for the application
    - `app` is the name of the application
    - `embedding_dim` is the dimension of the `embedding` column
  """
  def __init__(self, app, embedding_dim):
    self.app = app
    self.embedding_dim = embedding_dim


def img2b64(img_path):
  """
  Converts an image path to a base64 encoded string
  """
  _, file_extension = os.path.splitext(img_path)
  file_extension = file_extension[1:]
  with open(img_path, 'rb') as fp:
    img_b64 = base64.b64encode(fp.read()).decode('utf-8')
    img_b64 = f'data:image/{file_extension};base64,{img_b64}'
  return img_b64


def construct_cohere_contents(contents):
  """
  Constructs cohere specific contents given input of the form:
    [
      [
        {'text': },
        {'image': }
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
      elif actual_content.get('image'):
        cohere_content['content'].append({'type': 'image_url', 'image_url': {'url': img2b64(actual_content['image'])}})
      else:
        raise ValueError('Invalid content type!')
    cohere_contents.append(cohere_content)
  return cohere_contents
