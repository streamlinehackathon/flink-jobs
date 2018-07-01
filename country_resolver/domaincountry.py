import csv

import sys
import requests
import json
import whois
import pythonwhois as pw

key="27d4faa3d051c554ff25d3d64452a305"
infile = "180-days.csv"
outfile = "/tmp/domaincountries.csv"

out = open(outfile, "w")

with open(infile, 'r') as csvfile:
    domains = csv.reader(csvfile)
    for row in domains:
        try:
            domain = requests.get('http://api.ipstack.com/' + row[0]+'?access_key=' + key)
            r = str.strip(row[0]) + ","  + str(domain.json()['country_code'])
            out.writelines(r + "\n")
            print(r)

        except:
            print("Domain: ", str.strip(row[0]), "; " + "Unexpected error:", sys.exc_info())

out.close()