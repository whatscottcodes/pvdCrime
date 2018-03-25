import requests
import pandas as pd

headers = {"Authentication":"SkncRK5EwmLg8NSQwi0vAVhkc"}

response = requests.get("https://data.providenceri.gov/resource/gfyp-tfg9.json",headers=headers)

response_json = response.json()

pvd_crimes =  pd.read_json(response_json)
