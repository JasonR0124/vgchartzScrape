from bs4 import BeautifulSoup
import urllib.request
import time
import glob
import re
import os
import pandas as pd
import numpy as np

# This will check the files and determine the correct start page in case of restarting
startfiles = glob.glob('./vgsales_backup_*.csv')
startpage = 0
for file in startfiles:
    pagenum = int(re.findall('[0-9]+', file)[0])
    if pagenum > startpage:
        startpage = pagenum
startpage += 1


# just add to the pages number if more are added
def fetch_data_from_vgchartz(pages=65, max_retries=5, retry_delay=5):
    rec_count = 0
    records = []
    urlhead = 'http://www.vgchartz.com/gamedb/?page='
    urltail = '&console=&region=All&developer=&publisher=&genre=&boxart=Both&ownership=Both'
    urltail += '&results=1000&order=Sales&showtotalsales=0&showtotalsales=1&showpublisher=0'
    urltail += '&showpublisher=1&showvgchartzscore=0&shownasales=1&showdeveloper=1&showcriticscore=1'
    urltail += '&showpalsales=0&showpalsales=1&showreleasedate=1&showuserscore=1&showjapansales=1'
    urltail += '&showlastupdate=0&showothersales=1&showgenre=1&sort=GL'

    for page in range(startpage, pages):
        try:
            surl = urlhead + str(page) + urltail
            r = urllib.request.urlopen(surl).read()
            soup = BeautifulSoup(r, features="lxml")
            print(f"Page: {page}")

            game_tags = list(filter(
                lambda x: 'href' in x.attrs and x.attrs['href'].startswith('https://www.vgchartz.com/game/'),
                soup.find_all("a")[10:]
            ))
            
            for tag in game_tags:
                retries = 0
                while retries < max_retries:
                    try:
                        # Fetch data for a single game
                        game_data = fetch_game_data(tag)
                        records.append(game_data)
                        rec_count += 1
                        print(f"Record {rec_count}: {game_data['Name']}")
                        time.sleep(0.6)           # This prevents a 429 error
                        break
                    except Exception as e:
                        print(f"Error fetching data for game {tag.string}: {str(e)}")
                        print(f"Retrying game {tag.string}...")
                        retries += 1
                        time.sleep(retry_delay)

                if retries == max_retries:
                    print(f"Max retries exceeded for game {tag.string}. Skipping...")

            # Save backup after each page and clear records to free up memory and prevent crashing
            df = pd.DataFrame(records)
            df.to_csv(f"vgsales_backup_page_{page}.csv", sep=",", encoding='utf-8', index=False)
            df = None
            records = []

        except Exception as e:
            print(f"Error while scraping page {page}: {e}")
            continue

    print(f"Total records: {rec_count}")
    print(f"Total pages: {pages}")  
    return records


def fetch_game_data(tag):
    data = tag.parent.parent.find_all('td')
    platform_finder = data[3].find('img').attrs['alt']
    platforms = None
    gamesinseries = None
    url_to_game = tag.attrs['href']
    sitexml = urllib.request.urlopen(url_to_game).read()
    plat_soup = BeautifulSoup(sitexml, 'html.parser')
    # This checks the summary table as most games have the platforms listed on this table
    if platform_finder == 'All':
        summary_info_box = plat_soup.find('div', {'id': 'gameBody'})
        summary_info_tag = summary_info_box.find_next('p')
        sumstr = summary_info_tag.get_text(strip=True)
        consoles = re.findall('.+released on (.*)\\.', sumstr)
        all_games = ''
        # This is in case nothing is listed in the summary table it grabs from the Other Versions tag 
        # This unfortunately does not always have the full list
        if consoles == []:
            console_info_box = plat_soup.find('div', {'id': 'gameGenInfoBox'})
            console_info_heading = console_info_box.find('h2', string='Other Versions')
            if console_info_heading == None:
                all_platforms = ''
            else:
                console_info_tags = console_info_heading.find_next('p').find_all('a')
                for console in console_info_tags:
                    console = console.get_text(strip=True)
                    if platforms == None:
                        platforms = console
                    else:
                        platforms = platforms + '|' + console
                all_platforms = platforms
        else:
            consoles = consoles[0].replace(' and', '').replace(', ', '|')
            all_platforms = consoles
   # This is grabbing the series titles from the summary tab         
    elif platform_finder == 'Series':
        summary_info_box = plat_soup.find('div', {'id': 'gameBody'})
        series_game_tags = summary_info_box.find_all('li')
        for game in series_game_tags:
            gametag = game.find_all('a')
            if gametag == []:
                game = game.get_text(strip=True)
                if gamesinseries == None:
                    gamesinseries = game
                else:
                    gamesinseries = gamesinseries + '|' + game
            else:
                game = gametag[0].get_text(strip=True)
                if gamesinseries == None:
                    gamesinseries = game
                else:
                    gamesinseries = gamesinseries + '|' + game

        all_platforms = ''
        all_games = gamesinseries
    else:
        all_platforms = ''
        all_games = ''
    record = {
        'Rank': np.int32(data[0].string),
        'Name': " ".join(tag.string.split()),
        'Platform': data[3].find('img').attrs['alt'],
        'All_Platforms': all_platforms,
        'All_Games': all_games,
        'Publisher': data[4].string,
        'Developer': data[5].string,
        'Critic_Score': float(data[6].string) if not data[6].string.startswith("N/A") else np.nan,
        'User_Score': float(data[7].string) if not data[7].string.startswith("N/A") else np.nan,
        'NA_Sales': float(data[9].string[:-1]) if not data[9].string.startswith("N/A") else np.nan,
        'PAL_Sales': float(data[10].string[:-1]) if not data[10].string.startswith("N/A") else np.nan,
        'JP_Sales': float(data[11].string[:-1]) if not data[11].string.startswith("N/A") else np.nan,
        'Other_Sales': float(data[12].string[:-1]) if not data[12].string.startswith("N/A") else np.nan,
        'Global_Sales': float(data[8].string[:-1]) if not data[8].string.startswith("N/A") else np.nan,
    }

    release_year = data[13].string.split()[-1]
    if release_year.startswith('N/A'):
        record['Year'] = 'N/A'
    else:
        if int(release_year) >= 70:
            year_to_add = np.int32("19" + release_year)
        else:
            year_to_add = np.int32("20" + release_year)
        record['Year'] = year_to_add

    url_to_game = tag.attrs['href']
    site_raw = urllib.request.urlopen(url_to_game).read()
    sub_soup = BeautifulSoup(site_raw, "html.parser")
    genre_tag = sub_soup.find("div", {"id": "gameGenInfoBox"}).find('h2', string='Genre')
    record['Genre'] = genre_tag.next_sibling.string if genre_tag else "N/A"

    return record

def main():
    fetch_data_from_vgchartz()      # This initiates the script by calling the first function
    
    # This will run after all the info is gathered or if error scraping page
    files = glob.glob('./vgsales_backup_*.csv')    # Grabs all .csv file in working directory with names vgsales_backup_
    files.sort(key=lambda fname: int(''.join(filter(str.isdigit, fname))))  # Sort files by integer in filename
    dframe_lst = []

    for file in files:
        dframe = pd.read_csv(file)
        dframe_lst.append(dframe)
    
    # Check if a combined file exists already and if it does delete it. 
    if os.path.exists('VG_Sales_All.csv'):
        os.remove('VG_Sales_All.csv')
    
    full_dframe = pd.concat(dframe_lst, ignore_index=True)  # Combine list of dataframes into one dataframe
    full_dframe.to_csv('VG_Sales_All.csv', sep=',', encoding='utf-8', index=False)

if __name__ == "__main__":
    main()
