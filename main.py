import json

import numpy as np
import requests
import geopy.distance
import argparse
import aiohttp
import asyncio
import openpyxl
import pandas as pd
import io

ausgangs_plz : str
def lines_that_contain(string, fp):
    return [line for line in fp if string in line]
def get_random_token():
    api_url = "https://www.aponet.de/typo3temp/assets/compressed/pharmacymap-6a6e01d2abe96a6dc9b2a745f818f5a3.js?1693872002"
    response = requests.get(api_url)
    f = io.StringIO(response.content.decode("UTF-8"))
    random_token = lines_that_contain("var randomToken", f)[0]
    s1 = random_token.find('\'') + 1
    s2 = random_token.find('\'', s1 )
    random_token = random_token[s1 : s2]
    print(s1)
    print(s2)
    print(random_token)



def get_state_by_plz(plz : str):
    print(ausgangs_plz)
    api_url = f"https://zip-api.eu/api/v1/codes/postal_code={plz}"
    response = requests.get(api_url)

    if response.ok:
        plz_info_list = response.json()
        for plz_info in plz_info_list:
            if plz_info["country_code"] == "DE":
                return plz_info["state"]
    else:
        return False

def clean_plz_list(plz_list : list):
    plz_in_list = set()

    for plz in plz_list:
        if plz["postal_code"] in plz_in_list:
            plz_list.remove(plz)
        else:
            plz_in_list.add(plz["postal_code"])

    return plz_list
    print(len(plz_list))



def get_all_plz_for_state(state : str):
    api_url = f"https://zip-api.eu/api/v1/codes/state={state}"
    response = requests.get(api_url)

    plz_set = set()
    if response.ok:
        for plz in response.json():
            plz_set.add(plz["postal_code"])

        return plz_set
    else:
        print(f"Fehler bei Akquierierung der PLZ: {response.status_code} \n{response.content}")

        return False

def get_plz_cooridnates(plz : str):
    api_url = f"https://zip-api.eu/api/v1/codes/postal_code={plz}"

    response = requests.get(api_url)
    response = response.json()
    for place in response:
        if place["country_code"] == "DE":
            coorindates = (float(place["lat"]), float(place["lng"]))
            return coorindates



def get_tasks(session, plz_list, radius):
    tasks = []
    for plz in plz_list:
        apo_info = session.get(
            f"https://www.aponet.de/apotheke/apothekensuche?tx_aponetpharmacy_search[action]=result&tx_aponetpharmacy_search[search][plzort]={plz}&tx_aponetpharmacy_search[search][radius]={'02'}&tx_aponetpharmacy_search[token]=uRJRFV5qULs&type=1981&limit=100")
        print("Task added")
        tasks.append(apo_info)
    return tasks

async def get_apos():

    #Radius auf eins, weil die API maximal 50 Apotheken auf einmal zurückgibt und man eh jede
    #PLZ einmal abfragt
    radius = 1
    gesamt = 0
    state = get_state_by_plz(ausgangs_plz)
    plz_list = get_all_plz_for_state(state)
    apotheken = []
    async with aiohttp.ClientSession() as session:

        tasks = get_tasks(session, plz_list, radius)
        responses = await asyncio.gather(*tasks)
        for response in responses:
            response_src = await response.json()

            apotheken_list = response_src["results"]["apotheken"]
            if "apotheke" in apotheken_list:
                apotheken.extend(apotheken_list["apotheke"])

    return apotheken

def create_excel_file(apotheken : list):
    apotheken_frame = pd.DataFrame(apotheken)
    apotheken_frame = apotheken_frame.drop_duplicates(subset=["apo_id"])

    columns_to_keep = ['name', 'id', "strasse", "plz", "ort", "telefon", "email", "homepage", "longitude", "latitude"]
    apotheken_frame =  apotheken_frame.loc[:, columns_to_keep]

    apotheken_frame["distanz"] = np.NAN
    apotheken_frame["Letzter besuch"] = np.NAN
    apotheken_frame["Letztes Medikament"] = np.NAN

    plz_coordinates = get_plz_cooridnates(ausgangs_plz)

    lat_index = apotheken_frame.columns.get_loc("latitude") + 1
    long_index = apotheken_frame.columns.get_loc("longitude") + 1

    for row in apotheken_frame.itertuples():
        apo_coordinates = (row[lat_index], row[long_index])
        distance = geopy.distance.geodesic(plz_coordinates, apo_coordinates).km
        apotheken_frame.at[row[0], "distanz"] = round(distance, 2)

    apotheken_frame = apotheken_frame.sort_values("distanz")
    apotheken_frame.to_excel("apotheke.xlsx", sheet_name="Apos")


def main():

    parser =  argparse.ArgumentParser()

    parser.add_argument("-c", "--create", type=str, help="Create an excel file with all Pharmacies")

    args = parser.parse_args()

    if args.create:
        global ausgangs_plz
        ausgangs_plz = args.create
        apotheken = asyncio.run(get_apos())
        create_excel_file(apotheken)
    else:
        parser.print_help()


  #


def get_plzs(Bundesland : str):
    url = f"https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/georef-germany-postleitzahl/records?select=plz_code&where=lan_name%20%3D%20%22{Bundesland}%22&limit=100"
    response = requests.get(url).json()

    plzs = []
    for plz in response["results"]:
        plzs.append(plz["plz_code"])
        print(plz["plz_code"])

    return plzs


if __name__ == '__main__':
    main()
   # get_random_token()


