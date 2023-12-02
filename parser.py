import requests
from bs4 import BeautifulSoup
import re


def extract_lanuage_country(input_string):
    '''
    Example:
        input_string = "ukrainian (latin, Antigua & Barbuda)"   -> ('ukrainian', 'Antigua & Barbuda')
        input_string = 'hello (one, two)'                       -> ('hello', 'two')
        input_string = 'hello (one, two three #@$@%@)'          -> ('hello', 'two three #@$@%@')
        input_string = "ukrainian (Ukraine)"                    -> ('ukrainian', 'Ukraine')
        input_string = "ukrainian,kaka (Western Sahara)"        -> ('ukrainian,kaka', 'Western Sahara')
        input_string = "ukrainian,kaka (Antigua & Barbuda)"     -> ('ukrainian,kaka', 'Antigua & Barbuda')
        input_string = "ukrainian" # None                       -> (None, None)
        input_string = 'Uzbek (Cyrillic) (Uzbekistan)'          -> (None, None)

    Returns:
        (language, country)
    '''
    # Count the occurrences of open and closed brackets
    open_bracket_count = input_string.count('(')
    closed_bracket_count = input_string.count(')')

    # Check if there are two sets of open and closed brackets
    if open_bracket_count >= 2 and closed_bracket_count >= 2:
        return None, None
        
    match = re.match(r'(\S+)\s*\((?:[^,]+,\s*)?([^,]+(?:,\s*[^,]+)*)\)', input_string)

    if match:
        language_word = match.group(1)
        country_word = match.group(2) 
        return language_word, country_word
    else:
        return None, None
    

def extract_country_locale_code(input_string):
    '''
    Example:
        Input: word1_word2, Result: ('word1', 'word2')
        Input: aaa_mmm_sss, Result: ('aaa', 'sss')
        Input: single_word, Result: ('single', 'word')
        Input: no_match_string, Result: ('no', 'string')
        Input: sdsd, Result: None

    Returns:
        (lang_code, country_code)
    '''
    words = input_string.split('_')

    if len(words) == 2:
        # If there are two words, return both
        return tuple(words)
    elif len(words) == 3:
        # If there are three words, return the first and last
        return words[0], words[2]
    else:
        # If the number of words is not 2 or 3, return None
        return None, None


def parse_site_for_title_akas(path_to_save):
    # URL of the website
    url = 'https://saimana.com/list-of-country-locale-code/'

    # Send a GET request to the URL
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the tbody with class 'row-hover'
        tbody = soup.find('tbody', class_='row-hover')

        # Initialize an empty list to store the extracted data
        data_matrix = []

        # Find all tr elements within the tbody
        rows = tbody.find_all('tr')

        data_matrix.append(['language_name', 'language_code', 'country_name',  'country_code'])
        # Iterate through each tr element
        for row in rows:
            # Find all td elements within the tr
            cols = row.find_all('td')

            # Extract text from the first and second td elements
            language_country_col = cols[0].text.strip()      # LANGUAGE (COUNTRY)
            country_locale_code_col = cols[1].text.strip()   # langCode_countryCode

            language_str, country_str = extract_lanuage_country(language_country_col)
            if (language_str == country_str == None): continue

            language_code, country_code = extract_country_locale_code(country_locale_code_col)
            if (language_code == country_code == None): continue


            # Append the values to the data matrix
            data_matrix.append([language_str, language_code, country_str, country_code])

        # Display the data matrix
        # for row in data_matrix:
        #     print(row)

        # Save the data to a text file
        with open(path_to_save, 'w') as file:
            for row in data_matrix:
                file.write('\t'.join(row) + '\n')
        print("Results saved to 'results.tsv'")

    else:
        print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
