import bs4
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

def extract_usages(soup):
    usages = soup.findAll('a', {"class": "usg"})
    if usages:
        usages_str = '|'.join(usage.text for usage in usages)
    else:
        usages_str = 'na'

    return usages_str

i = 0
def get_ethnicities(name):
    global i
    base_url = 'https://www.behindthename.com'
    url = 'https://www.behindthename.com/name/%s' % name
    html = requests.get(url).content
    soup = bs4.BeautifulSoup(html, 'lxml')
    usages_str = extract_usages(soup) == 'na'
    if usages_str == 'na':
        links = soup.findAll('a', {"href": "re.compile('/name/.*')"})
        if links:
            link = base_url + links[0]['href']
            html = requests.get(link).content
            soup = bs4.BeautifulSoup(html, "lxml")
            usages_str = extract_usages(soup)
    print('done for %s' % i)
    i += 1
    return usages_str

with open('names.txt', 'r') as f:
    names = [line.strip().upper() for line in f.readlines()]

names = list(set(names))
usages_list = []
with ThreadPoolExecutor(max_workers=10) as executor:
    usages_list = list(executor.map(get_ethnicities, names))

df = pd.DataFrame.from_dict({'name': names, 'usages':usages_list})
df.to_csv('output.csv')
