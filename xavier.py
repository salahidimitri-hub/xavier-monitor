#!/usr/bin/env python3
"""
xavier.py -- Xavier, Ava's twin. Complete and live, in ONE file.

Ava keeps the lamp. Xavier walks into the dark and marks where it hurts --
atrocities, major crimes, natural disasters, droughts, famine -- pinned on a
world map, each mark carrying the channels where help can come from.

Zero pip dependencies. Standard library only. To run:

    python xavier.py
    # then open http://localhost:8000

He will:
  1. pull exact-coordinate alerts from USGS (earthquakes), GDACS (floods,
     cyclones, volcanoes) and NASA EONET (wildfires, storms) -- all free,
     public, keyless
  2. read world news feeds and pin named cities (or the country's center
     when no city is named) for atrocities, major crimes, drought, famine
  3. store everything in a local SQLite file, deduplicated, 7-day window
  4. serve a live dark world map: color-coded marks, tap one and it opens
     with the report, Xavier's word, and REAL help channels -- only phone
     numbers that are verified and stable are ever shown
  5. speak, if you ask him to

DEPLOY (free): push to GitHub, then on Render / Fly.io use
start command:  python xavier.py   (reads the PORT env var automatically)
"""

import os
import re
import json
import time
import html
import sqlite3
import threading
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DB_PATH = os.environ.get("XAV_DB", "xavier.db")
PORT = int(os.environ.get("PORT", "8000"))
WINDOW_DAYS = 7
INGEST_EVERY_SECONDS = 60 * 30  # every 30 minutes

# ---------------------------------------------------------------------------
# COORDINATE FEEDS (exact pins) + NEWS FEEDS (text -> city/country pins)
# ---------------------------------------------------------------------------
USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_week.geojson"
GDACS_URL = "https://www.gdacs.org/xml/rss.xml"
EONET_URL = "https://eonet.gsfc.nasa.gov/api/v3/events?status=open&limit=80"

NEWS_FEEDS = [
    ("ReliefWeb", "https://reliefweb.int/updates/rss.xml"),
    ("UN News", "https://news.un.org/feed/subscribe/en/news/all/rss.xml"),
    ("HRW", "https://www.hrw.org/rss/news"),
    ("Amnesty", "https://www.amnesty.org/en/latest/news/rss/"),
    ("Al Jazeera", "https://www.aljazeera.com/xml/rss/all.xml"),
    ("BBC World", "https://feeds.bbci.co.uk/news/world/rss.xml"),
    ("Guardian World", "https://www.theguardian.com/world/rss"),
    ("France 24", "https://www.france24.com/en/rss"),
]

# ---------------------------------------------------------------------------
# CATEGORIES -- keywords that classify a report. First match (in this order)
# wins, so the gravest categories are checked first.
# ---------------------------------------------------------------------------
CATEGORIES = [
    ("Atrocity", 5, ["massacre","genocide","mass grave","mass graves","war crime",
        "war crimes","ethnic cleansing","crimes against humanity","executed civilians",
        "summary execution","extrajudicial","atrocity","atrocities","civilians killed",
        "airstrike kills","shelling kills","chemical attack","torture"]),
    ("Famine", 5, ["famine","starvation","starving","acute malnutrition","hunger crisis",
        "food crisis","catastrophic hunger","denied aid","aid blocked","humanitarian blockade"]),
    ("Major Crime", 4, ["mass shooting","terror attack","terrorist attack","suicide bombing",
        "car bomb","bombing kills","hostage","hostages","kidnapped","kidnapping","abducted",
        "hijacked","gunmen killed","cartel","human trafficking","school shooting","stabbing attack"]),
    ("Disaster", 4, ["earthquake","tsunami","hurricane","typhoon","cyclone","flood","floods",
        "flooding","flash flood","wildfire","wildfires","landslide","mudslide","volcano",
        "volcanic eruption","tornado","storm kills","heatwave kills","avalanche","dam collapse",
        "building collapse","plane crash","ferry sank","capsized"]),
    ("Drought", 3, ["drought","water crisis","water shortage","crops failed","failed harvest",
        "livestock died","reservoirs dry","aquifer"]),
]
_CAT_COMPILED = [(name, sev, [re.compile(r"\b" + re.escape(k) + r"\b", re.I) for k in kws])
                 for name, sev, kws in CATEGORIES]

_ESCALATE = [re.compile(r"\b" + k + r"\b", re.I) for k in
             ["hundreds (dead|killed)", "thousands (dead|killed|displaced)",
              "dozens (dead|killed)", r"\d{2,} (dead|killed)"]]


def classify(text):
    """Return (category, severity) or (None, 0)."""
    for name, sev, pats in _CAT_COMPILED:
        if any(p.search(text) for p in pats):
            if any(p.search(text) for p in _ESCALATE):
                sev = min(5, sev + 1)
            return name, sev
    return None, 0


# ---------------------------------------------------------------------------
# GAZETTEER -- named cities pin exactly; otherwise the country's center.
# Approximate coordinates; good enough for a world map pin.
# ---------------------------------------------------------------------------
CITIES = {
 "Kabul":("Afghanistan",34.53,69.17),"Kandahar":("Afghanistan",31.62,65.72),
 "Aleppo":("Syria",36.20,37.16),"Damascus":("Syria",33.51,36.29),"Idlib":("Syria",35.93,36.63),
 "Homs":("Syria",34.73,36.72),"Gaza":("Palestine",31.50,34.47),"Rafah":("Palestine",31.29,34.25),
 "Khan Younis":("Palestine",31.34,34.31),"Jerusalem":("Israel",31.77,35.21),
 "Tel Aviv":("Israel",32.08,34.78),"Beirut":("Lebanon",33.89,35.50),
 "Baghdad":("Iraq",33.31,44.36),"Mosul":("Iraq",36.34,43.13),"Basra":("Iraq",30.51,47.78),
 "Tehran":("Iran",35.69,51.39),"Sanaa":("Yemen",15.35,44.21),"Aden":("Yemen",12.79,45.03),
 "Hodeidah":("Yemen",14.80,42.95),"Khartoum":("Sudan",15.50,32.56),
 "Omdurman":("Sudan",15.64,32.48),"El Fasher":("Sudan",13.63,25.35),"Nyala":("Sudan",12.05,24.88),
 "Port Sudan":("Sudan",19.62,37.22),"Juba":("South Sudan",4.85,31.60),
 "Addis Ababa":("Ethiopia",9.02,38.75),"Mekelle":("Ethiopia",13.50,39.47),
 "Mogadishu":("Somalia",2.05,45.32),"Nairobi":("Kenya",-1.29,36.82),
 "Kampala":("Uganda",0.35,32.58),"Goma":("Congo",-1.66,29.22),"Bukavu":("Congo",-2.49,28.86),
 "Kinshasa":("Congo",-4.33,15.31),"Bangui":("Central African Republic",4.36,18.55),
 "Kano":("Nigeria",12.00,8.52),"Maiduguri":("Nigeria",11.83,13.15),
 "Lagos":("Nigeria",6.52,3.38),"Abuja":("Nigeria",9.06,7.49),
 "Ouagadougou":("Burkina Faso",12.37,-1.52),"Bamako":("Mali",12.64,-8.00),
 "Timbuktu":("Mali",16.77,-3.00),"Niamey":("Niger",13.51,2.13),
 "Port-au-Prince":("Haiti",18.55,-72.34),
 "Kyiv":("Ukraine",50.45,30.52),"Kharkiv":("Ukraine",49.99,36.23),"Odesa":("Ukraine",46.48,30.73),
 "Mariupol":("Ukraine",47.10,37.55),"Donetsk":("Ukraine",48.00,37.80),
 "Zaporizhzhia":("Ukraine",47.84,35.14),"Kherson":("Ukraine",46.64,32.61),
 "Moscow":("Russia",55.76,37.62),"Belgorod":("Russia",50.60,36.58),
 "Sittwe":("Myanmar",20.15,92.90),"Yangon":("Myanmar",16.87,96.20),
 "Mandalay":("Myanmar",21.98,96.08),"Dhaka":("Bangladesh",23.81,90.41),
 "Kathmandu":("Nepal",27.72,85.32),"Islamabad":("Pakistan",33.69,73.06),
 "Karachi":("Pakistan",24.86,67.00),"Lahore":("Pakistan",31.55,74.34),
 "Peshawar":("Pakistan",34.01,71.58),"Quetta":("Pakistan",30.18,66.98),
 "Delhi":("India",28.61,77.21),"Mumbai":("India",19.08,72.88),"Kolkata":("India",22.57,88.36),
 "Chennai":("India",13.08,80.27),"Colombo":("Sri Lanka",6.93,79.85),
 "Beijing":("China",39.90,116.40),"Shanghai":("China",31.23,121.47),
 "Hong Kong":("China",22.32,114.17),"Taipei":("Taiwan",25.03,121.57),
 "Manila":("Philippines",14.60,120.98),"Jakarta":("Indonesia",-6.21,106.85),
 "Bangkok":("Thailand",13.76,100.50),"Phnom Penh":("Cambodia",11.56,104.92),
 "Hanoi":("Vietnam",21.03,105.85),"Seoul":("South Korea",37.57,126.98),
 "Pyongyang":("North Korea",39.03,125.75),"Tokyo":("Japan",35.68,139.69),
 "Osaka":("Japan",34.69,135.50),"Sydney":("Australia",-33.87,151.21),
 "Melbourne":("Australia",-37.81,144.96),"Auckland":("New Zealand",-36.85,174.76),
 "London":("United Kingdom",51.51,-0.13),"Manchester":("United Kingdom",53.48,-2.24),
 "Paris":("France",48.86,2.35),"Marseille":("France",43.30,5.37),
 "Berlin":("Germany",52.52,13.41),"Munich":("Germany",48.14,11.58),
 "Madrid":("Spain",40.42,-3.70),"Barcelona":("Spain",41.39,2.17),
 "Lisbon":("Portugal",38.72,-9.14),"Rome":("Italy",41.90,12.50),"Milan":("Italy",45.46,9.19),
 "Athens":("Greece",37.98,23.73),"Istanbul":("Turkey",41.01,28.98),
 "Ankara":("Turkey",39.93,32.87),"Gaziantep":("Turkey",37.07,37.38),
 "Antakya":("Turkey",36.20,36.16),"Cairo":("Egypt",30.04,31.24),
 "Alexandria":("Egypt",31.20,29.92),"Tripoli":("Libya",32.89,13.19),
 "Benghazi":("Libya",32.12,20.07),"Derna":("Libya",32.77,22.64),
 "Tunis":("Tunisia",36.80,10.18),"Algiers":("Algeria",36.75,3.06),
 "Casablanca":("Morocco",33.57,-7.59),"Marrakesh":("Morocco",31.63,-8.00),
 "Dakar":("Senegal",14.72,-17.47),"Freetown":("Sierra Leone",8.48,-13.23),
 "Monrovia":("Liberia",6.29,-10.76),"Accra":("Ghana",5.56,-0.20),
 "Abidjan":("Ivory Coast",5.36,-4.01),"Johannesburg":("South Africa",-26.20,28.05),
 "Cape Town":("South Africa",-33.92,18.42),"Durban":("South Africa",-29.86,31.02),
 "Harare":("Zimbabwe",-17.83,31.05),"Lusaka":("Zambia",-15.39,28.32),
 "Lilongwe":("Malawi",-13.96,33.77),"Maputo":("Mozambique",-25.97,32.57),
 "Beira":("Mozambique",-19.83,34.85),"Antananarivo":("Madagascar",-18.88,47.51),
 "Caracas":("Venezuela",10.48,-66.90),"Bogota":("Colombia",4.71,-74.07),
 "Medellin":("Colombia",6.24,-75.58),"Quito":("Ecuador",-0.18,-78.47),
 "Guayaquil":("Ecuador",-2.19,-79.89),"Lima":("Peru",-12.05,-77.04),
 "La Paz":("Bolivia",-16.50,-68.15),"Santiago":("Chile",-33.45,-70.67),
 "Buenos Aires":("Argentina",-34.60,-58.38),"Sao Paulo":("Brazil",-23.55,-46.63),
 "Rio de Janeiro":("Brazil",-22.91,-43.17),"Brasilia":("Brazil",-15.79,-47.88),
 "Asuncion":("Paraguay",-25.26,-57.58),"Montevideo":("Uruguay",-34.90,-56.16),
 "Mexico City":("Mexico",19.43,-99.13),"Guadalajara":("Mexico",20.66,-103.35),
 "Tijuana":("Mexico",32.51,-117.04),"Ciudad Juarez":("Mexico",31.69,-106.42),
 "Guatemala City":("Guatemala",14.63,-90.51),"San Salvador":("El Salvador",13.69,-89.19),
 "Tegucigalpa":("Honduras",14.07,-87.19),"Managua":("Nicaragua",12.11,-86.24),
 "Havana":("Cuba",23.11,-82.37),"Kingston":("Jamaica",17.97,-76.79),
 "New York":("United States",40.71,-74.01),"Los Angeles":("United States",34.05,-118.24),
 "Chicago":("United States",41.88,-87.63),"Houston":("United States",29.76,-95.37),
 "Miami":("United States",25.76,-80.19),"New Orleans":("United States",29.95,-90.07),
 "San Francisco":("United States",37.77,-122.42),"Seattle":("United States",47.61,-122.33),
 "Toronto":("Canada",43.65,-79.38),"Vancouver":("Canada",49.28,-123.12),
 "Montreal":("Canada",45.50,-73.57),
}

COUNTRY_CENTERS = {
 "Afghanistan":(33.0,66.0),"Albania":(41.0,20.0),"Algeria":(28.0,3.0),"Angola":(-12.0,17.0),
 "Argentina":(-34.0,-64.0),"Armenia":(40.0,45.0),"Australia":(-25.0,134.0),"Austria":(47.5,14.0),
 "Azerbaijan":(40.3,47.5),"Bahrain":(26.0,50.5),"Bangladesh":(24.0,90.0),"Belarus":(53.0,28.0),
 "Belgium":(50.7,4.5),"Bolivia":(-17.0,-65.0),"Bosnia":(44.0,18.0),"Brazil":(-10.0,-52.0),
 "Bulgaria":(42.7,25.0),"Burkina Faso":(12.5,-1.5),"Burundi":(-3.4,29.9),"Cambodia":(12.5,105.0),
 "Cameroon":(5.7,12.7),"Canada":(56.0,-106.0),"Central African Republic":(6.6,20.9),
 "Chad":(15.4,18.7),"Chile":(-30.0,-71.0),"China":(35.0,103.0),"Colombia":(4.0,-73.0),
 "Congo":(-2.9,23.6),"Croatia":(45.2,15.5),"Cuba":(21.5,-79.5),"Cyprus":(35.0,33.0),
 "Czech":(49.8,15.5),"Denmark":(56.0,10.0),"Djibouti":(11.6,42.6),"Ecuador":(-1.5,-78.5),
 "Egypt":(26.5,30.0),"El Salvador":(13.8,-88.9),"Eritrea":(15.2,39.0),"Estonia":(58.7,25.0),
 "Ethiopia":(9.0,39.6),"Finland":(64.0,26.0),"France":(46.5,2.5),"Gabon":(-0.6,11.6),
 "Gambia":(13.4,-15.4),"Georgia":(42.0,43.5),"Germany":(51.0,10.4),"Ghana":(7.9,-1.2),
 "Greece":(39.0,22.0),"Guatemala":(15.5,-90.3),"Guinea":(10.4,-10.9),"Haiti":(19.0,-72.4),
 "Honduras":(14.8,-86.6),"Hungary":(47.2,19.4),"India":(22.0,79.0),"Indonesia":(-2.5,118.0),
 "Iran":(32.4,53.7),"Iraq":(33.0,43.7),"Ireland":(53.4,-8.0),"Israel":(31.4,35.0),
 "Italy":(42.8,12.8),"Ivory Coast":(7.5,-5.5),"Jamaica":(18.1,-77.3),"Japan":(36.5,138.0),
 "Jordan":(31.3,36.4),"Kazakhstan":(48.0,67.0),"Kenya":(0.5,37.9),"Kosovo":(42.6,20.9),
 "Kuwait":(29.3,47.6),"Kyrgyzstan":(41.5,74.5),"Laos":(18.5,103.9),"Latvia":(56.9,24.9),
 "Lebanon":(33.9,35.9),"Liberia":(6.4,-9.4),"Libya":(27.0,17.3),"Lithuania":(55.2,23.9),
 "Madagascar":(-19.4,46.7),"Malawi":(-13.2,34.3),"Malaysia":(3.8,109.0),"Maldives":(3.2,73.2),
 "Mali":(17.3,-3.5),"Mauritania":(20.3,-10.3),"Mexico":(23.9,-102.5),"Moldova":(47.2,28.5),
 "Mongolia":(46.8,103.1),"Montenegro":(42.8,19.3),"Morocco":(31.9,-6.9),"Mozambique":(-17.3,35.5),
 "Myanmar":(21.0,96.5),"Namibia":(-22.0,17.2),"Nepal":(28.3,84.1),"Netherlands":(52.2,5.3),
 "New Zealand":(-41.8,172.8),"Nicaragua":(12.9,-85.2),"Niger":(17.4,9.4),"Nigeria":(9.6,8.1),
 "North Korea":(40.1,127.2),"Norway":(64.5,11.0),"Oman":(20.6,56.1),"Pakistan":(30.0,69.3),
 "Palestine":(31.9,35.2),"Panama":(8.5,-80.1),"Paraguay":(-23.2,-58.4),"Peru":(-9.2,-74.4),
 "Philippines":(12.9,121.8),"Poland":(52.1,19.4),"Portugal":(39.6,-8.0),"Qatar":(25.3,51.2),
 "Romania":(45.8,25.0),"Russia":(61.5,99.0),"Rwanda":(-2.0,29.9),"Saudi Arabia":(24.0,44.5),
 "Senegal":(14.4,-14.5),"Serbia":(44.0,20.9),"Sierra Leone":(8.5,-11.8),"Singapore":(1.35,103.8),
 "Slovakia":(48.7,19.5),"Slovenia":(46.1,14.8),"Somalia":(6.0,46.0),"South Africa":(-29.0,25.0),
 "South Korea":(36.4,127.9),"South Sudan":(7.3,30.3),"Spain":(40.2,-3.6),"Sri Lanka":(7.6,80.7),
 "Sudan":(15.5,30.0),"Sweden":(62.8,16.7),"Switzerland":(46.8,8.2),"Syria":(35.0,38.5),
 "Taiwan":(23.7,121.0),"Tajikistan":(38.9,71.3),"Tanzania":(-6.4,34.9),"Thailand":(15.0,101.0),
 "Togo":(8.5,1.0),"Tunisia":(34.1,9.6),"Turkey":(39.0,35.4),"Turkmenistan":(39.0,59.4),
 "Uganda":(1.3,32.4),"Ukraine":(49.0,31.4),"United Arab Emirates":(24.0,54.3),
 "United Kingdom":(54.0,-2.9),"United States":(39.8,-98.6),"Uruguay":(-32.8,-56.0),
 "Uzbekistan":(41.7,63.7),"Venezuela":(7.1,-66.2),"Vietnam":(16.6,106.3),"Yemen":(15.9,47.9),
 "Zambia":(-13.5,27.8),"Zimbabwe":(-19.0,29.9),
}

_CITY_COMPILED = sorted(
    [(name, re.compile(r"\b" + re.escape(name) + r"\b"), data) for name, data in CITIES.items()],
    key=lambda x: -len(x[0]))
_COUNTRY_COMPILED = sorted(
    [(name, re.compile(r"\b" + re.escape(name) + r"\b"), c) for name, c in COUNTRY_CENTERS.items()],
    key=lambda x: -len(x[0]))


def geolocate(text):
    """(place, country, lat, lon, precision) from a passage, or None.

    Named city -> exact city pin. Otherwise named country -> its center.
    Longer names first, so South Sudan never mispins as Sudan."""
    for name, rx, (country, lat, lon) in _CITY_COMPILED:
        if rx.search(text):
            return name, country, lat, lon, "city"
    matched = []
    covered = []
    for name, rx, (lat, lon) in _COUNTRY_COMPILED:
        m = rx.search(text)
        if m and not any(s <= m.start() < e for s, e in covered):
            matched.append((name, lat, lon, m.start()))
            covered.append((m.start(), m.end()))
    if matched:
        matched.sort(key=lambda x: x[3])
        name, lat, lon, _ = matched[0]
        return name, name, lat, lon, "country"
    return None


# ---------------------------------------------------------------------------
# HELP CHANNELS -- real, stable organizations. Phone numbers appear ONLY when
# verified and long-standing; everything else links by name/site.
# ---------------------------------------------------------------------------
HELP = {
 "Atrocity": [
  {"name":"UN OHCHR","what":"documents grave violations, can trigger investigations","contact":"ohchr.org"},
  {"name":"International Committee of the Red Cross (ICRC)","what":"protects victims of conflict","contact":"icrc.org · +41 22 734 60 01 (Geneva HQ)"},
  {"name":"International Criminal Court","what":"war crimes, crimes against humanity, genocide","contact":"icc-cpi.int"},
  {"name":"Human Rights Watch","what":"investigates and reports abuses","contact":"hrw.org"},
 ],
 "Major Crime": [
  {"name":"Local emergency services","what":"immediate danger to life","contact":"see emergency number below"},
  {"name":"INTERPOL","what":"international crime coordination","contact":"interpol.int"},
  {"name":"UNODC","what":"UN office on drugs, crime and trafficking","contact":"unodc.org"},
  {"name":"ICRC Restoring Family Links","what":"finding family separated by crisis","contact":"familylinks.icrc.org"},
 ],
 "Disaster": [
  {"name":"Local emergency services","what":"immediate danger to life","contact":"see emergency number below"},
  {"name":"IFRC / Red Cross Red Crescent","what":"first-line disaster relief worldwide","contact":"ifrc.org"},
  {"name":"UN OCHA","what":"coordinates international humanitarian response","contact":"unocha.org · reliefweb.int"},
  {"name":"American Red Cross","what":"US disaster help line","contact":"redcross.org · 1-800-733-2767"},
  {"name":"FEMA (US)","what":"US federal disaster assistance","contact":"fema.gov · 1-800-621-3362"},
  {"name":"Disaster Distress Helpline (US)","what":"24/7 emotional support after disasters","contact":"1-800-985-5990"},
 ],
 "Drought": [
  {"name":"World Food Programme","what":"food assistance in drought and crisis","contact":"wfp.org"},
  {"name":"UNICEF","what":"water, sanitation and child nutrition","contact":"unicef.org"},
  {"name":"FAO","what":"agriculture and livelihood recovery","contact":"fao.org"},
 ],
 "Famine": [
  {"name":"World Food Programme","what":"emergency food assistance","contact":"wfp.org"},
  {"name":"UNICEF","what":"treats acute child malnutrition","contact":"unicef.org"},
  {"name":"Action Against Hunger","what":"hunger-focused relief in 50+ countries","contact":"actionagainsthunger.org"},
  {"name":"IPC","what":"official famine classification and alerts","contact":"ipcinfo.org"},
 ],
}

# Verified, stable national emergency numbers. Everywhere else: 112 is
# reachable from GSM mobile phones in most of the world.
EMERGENCY = {
 "United States":"911","Canada":"911","Mexico":"911","United Kingdom":"999","Ireland":"112 / 999",
 "Australia":"000","New Zealand":"111","India":"112","Pakistan":"15 (police) / 1122 (rescue)",
 "Bangladesh":"999","Japan":"110 (police) / 119 (fire, ambulance)","South Korea":"112 (police) / 119 (fire, ambulance)",
 "China":"110 (police) / 120 (ambulance)","Philippines":"911","Indonesia":"112","Thailand":"191",
 "South Africa":"10111 (police) / 10177 (ambulance)","Nigeria":"112","Kenya":"999 / 112",
 "Ghana":"112","Brazil":"190 (police) / 192 (ambulance)","Argentina":"911","Chile":"133 (police) / 131 (ambulance)",
 "Colombia":"123","Peru":"105","Venezuela":"911","France":"112","Germany":"112","Spain":"112",
 "Italy":"112","Portugal":"112","Netherlands":"112","Belgium":"112","Poland":"112","Ukraine":"112",
 "Turkey":"112","Greece":"112","Russia":"112","Egypt":"122 (police) / 123 (ambulance)",
 "Israel":"100 (police) / 101 (ambulance)","Saudi Arabia":"911","United Arab Emirates":"999",
}
EMERGENCY_DEFAULT = "112 (reachable from GSM mobile phones in most countries)"


def emergency_for(country):
    return EMERGENCY.get(country, EMERGENCY_DEFAULT)


# ---------------------------------------------------------------------------
# XAVIER -- Ava's twin. He goes where it is darkest and marks the spot.
# ---------------------------------------------------------------------------
XAVIER_INTRO = ("I am Xavier. Ava is my twin: she keeps the lamp, I walk into the dark "
                "and mark where it hurts. Every mark I make carries a way to help. "
                "Look at the map. Do not look away.")

XAVIER_LINES = {
 "Atrocity": "This mark is the heaviest kind I make. People did this to people. The record is kept, and the channels beside it exist to answer it.",
 "Major Crime": "Violence took lives or freedom here. If you are near, the emergency number below is the fastest hand to reach for.",
 "Disaster": "The earth or the sky struck here. The first hours matter most; the responders below are built for exactly this.",
 "Drought": "This harm moves slowly, which is why it is ignored. Water is failing here, and the harvest after it.",
 "Famine": "Hunger at this scale is never only weather. Aid exists, and the bodies below move it. What is missing is attention.",
}


# ---------------------------------------------------------------------------
# STORAGE
# ---------------------------------------------------------------------------
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with closing(db()) as conn, conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS events(
            uid TEXT PRIMARY KEY, title TEXT, category TEXT, severity INTEGER,
            place TEXT, country TEXT, lat REAL, lon REAL, precision TEXT,
            source TEXT, url TEXT, fetched_at TEXT)""")


# ---------------------------------------------------------------------------
# FETCHERS -- each fails silently; a dead feed contributes nothing.
# ---------------------------------------------------------------------------
def _get(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "Xavier/0.1"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def fetch_usgs():
    out = []
    try:
        data = json.loads(_get(USGS_URL).decode("utf-8", "replace"))
    except Exception:
        return out
    for f in data.get("features", []):
        try:
            p = f.get("properties") or {}
            lon, lat = (f.get("geometry") or {}).get("coordinates", [None, None])[:2]
            if lat is None:
                continue
            mag = p.get("mag") or 0
            sev = 5 if mag >= 7 else (4 if mag >= 6 else 3)
            out.append({"uid": "usgs:" + str(f.get("id")),
                        "title": p.get("title") or ("M" + str(mag) + " earthquake"),
                        "category": "Disaster", "severity": sev,
                        "place": p.get("place") or "", "country": "",
                        "lat": lat, "lon": lon, "precision": "exact",
                        "source": "USGS", "url": p.get("url") or ""})
        except Exception:
            continue
    return out


def _local(tag):
    return tag.split("}")[-1].lower()


def fetch_gdacs():
    out = []
    try:
        root = ET.fromstring(_get(GDACS_URL))
    except Exception:
        return out
    for el in root.iter():
        if _local(el.tag) != "item":
            continue
        title = link = point = ""
        for ch in el:
            ln = _local(ch.tag)
            if ln == "title":
                title = (ch.text or "").strip()
            elif ln == "link":
                link = (ch.text or "").strip()
            elif ln == "point":
                point = (ch.text or "").strip()
        if not title or not point:
            continue
        try:
            lat, lon = [float(x) for x in point.split()[:2]]
        except Exception:
            continue
        cat, sev = classify(title)
        out.append({"uid": "gdacs:" + (link or title),
                    "title": title, "category": cat or "Disaster",
                    "severity": sev or 3, "place": "", "country": "",
                    "lat": lat, "lon": lon, "precision": "exact",
                    "source": "GDACS", "url": link})
    return out


def fetch_eonet():
    out = []
    try:
        data = json.loads(_get(EONET_URL).decode("utf-8", "replace"))
    except Exception:
        return out
    for ev in data.get("events", []):
        try:
            geo = (ev.get("geometry") or [])
            if not geo:
                continue
            coords = geo[-1].get("coordinates")
            if isinstance(coords[0], list):      # polygon -> first vertex
                coords = coords[0][0] if isinstance(coords[0][0], list) else coords[0]
            lon, lat = coords[:2]
            cats = ",".join(c.get("title", "") for c in ev.get("categories", []))
            title = ev.get("title", "")
            cat, sev = classify(title + " " + cats)
            if cat is None:
                cat, sev = ("Drought", 3) if "drought" in cats.lower() else ("Disaster", 3)
            out.append({"uid": "eonet:" + str(ev.get("id")),
                        "title": title + (" (" + cats + ")" if cats else ""),
                        "category": cat, "severity": sev, "place": "", "country": "",
                        "lat": lat, "lon": lon, "precision": "exact",
                        "source": "NASA EONET", "url": (ev.get("sources") or [{}])[0].get("url", "")})
        except Exception:
            continue
    return out


def _strip_html(s):
    return html.unescape(re.sub("<[^>]+>", "", s or "")).strip()


def parse_feed_xml(data):
    out = []
    try:
        root = ET.fromstring(data)
    except Exception:
        return out
    for el in root.iter():
        if _local(el.tag) in ("item", "entry"):
            title = link = summary = ""
            for ch in el:
                ln = _local(ch.tag)
                if ln == "title":
                    title = (ch.text or "").strip()
                elif ln == "link":
                    link = (ch.text or ch.get("href") or "").strip()
                elif ln in ("description", "summary", "content") and not summary:
                    summary = _strip_html(ch.text or "")
            if title:
                out.append((title, link, summary))
    return out


def fetch_news():
    out = []
    for source, url in NEWS_FEEDS:
        try:
            items = parse_feed_xml(_get(url))
        except Exception:
            continue
        for title, link, summary in items:
            text = title + " " + summary
            cat, sev = classify(text)
            if cat is None:
                continue
            loc = geolocate(text)
            if loc is None:
                continue
            place, country, lat, lon, precision = loc
            out.append({"uid": "news:" + (link or title),
                        "title": title, "category": cat, "severity": sev,
                        "place": place if precision == "city" else "",
                        "country": country, "lat": lat, "lon": lon,
                        "precision": precision, "source": source, "url": link})
    return out


def ingest(events=None):
    init_db()
    items = events if events is not None else (
        fetch_usgs() + fetch_gdacs() + fetch_eonet() + fetch_news())
    added = 0
    now = datetime.now(timezone.utc).isoformat()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=WINDOW_DAYS)).isoformat()
    with closing(db()) as conn, conn:
        for e in items:
            if not e.get("uid") or e.get("lat") is None:
                continue
            cur = conn.execute(
                """INSERT OR IGNORE INTO events
                   (uid,title,category,severity,place,country,lat,lon,precision,source,url,fetched_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (e["uid"], e["title"][:300], e["category"], e["severity"],
                 e.get("place", ""), e.get("country", ""), e["lat"], e["lon"],
                 e.get("precision", "country"), e.get("source", ""), e.get("url", ""), now))
            added += cur.rowcount
        conn.execute("DELETE FROM events WHERE fetched_at < ?", (cutoff,))
        total = conn.execute("SELECT COUNT(*) c FROM events").fetchone()["c"]
    return {"seen": len(items), "added": added, "held": total}


def all_events():
    init_db()
    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT * FROM events ORDER BY severity DESC, fetched_at DESC LIMIT 500").fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["xavier"] = XAVIER_LINES.get(d["category"], "")
        d["help"] = HELP.get(d["category"], [])
        d["emergency"] = emergency_for(d["country"]) if d["country"] else EMERGENCY_DEFAULT
        out.append(d)
    return out


def background_ingest():
    while True:
        try:
            ingest()
        except Exception:
            pass
        time.sleep(INGEST_EVERY_SECONDS)


# ---------------------------------------------------------------------------
# WEB SERVER
# ---------------------------------------------------------------------------
class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body, ctype):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj, code=200):
        self._send(code, json.dumps(obj).encode("utf-8"),
                   "application/json; charset=utf-8")

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        try:
            if path == "/":
                self._send(200, PAGE.encode("utf-8"), "text/html; charset=utf-8")
            elif path == "/api/events":
                self._json(all_events())
            elif path == "/api/ingest":
                self._json(ingest())
            else:
                self._json({"error": "not found"}, 404)
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def log_message(self, *args):
        pass


PAGE = """<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Xavier — where it hurts</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.css">
<link href="https://fonts.googleapis.com/css2?family=Newsreader:ital@0;1&family=IBM+Plex+Mono&family=IBM+Plex+Sans:wght@400;600&display=swap" rel="stylesheet">
<style>
:root{--ink:#07090C;--char:#12161B;--line:#242A31;--bone:#E8E4DA;--dim:#9A948A;--steel:#7F8C97;--cold:#6FA8DC;
 --c-atro:#D64533;--c-crime:#E08A3C;--c-dis:#4A90D9;--c-dro:#D9B44A;--c-fam:#A97BE0}
*{box-sizing:border-box}html,body{height:100%}
body{margin:0;background:var(--ink);color:var(--bone);font-family:'IBM Plex Sans',sans-serif}
#map{position:fixed;inset:0;background:var(--ink)}
.leaflet-container{background:var(--ink)}
.panel{position:fixed;top:0;left:0;right:0;z-index:1000;padding:12px 14px 10px;
 background:linear-gradient(180deg,rgba(7,9,12,.94),rgba(7,9,12,.75) 75%,transparent);pointer-events:none}
.panel *{pointer-events:auto}
h1{font-family:'Newsreader',serif;font-weight:400;font-size:26px;margin:0;color:var(--bone)}
.eyebrow{font-family:'IBM Plex Mono',monospace;font-size:9px;letter-spacing:.2em;text-transform:uppercase;color:var(--steel);margin:0}
.intro{font-family:'Newsreader',serif;font-style:italic;font-size:13px;color:var(--dim);margin:4px 0 8px;max-width:560px}
.chips{display:flex;flex-wrap:wrap;gap:6px}
.chip{font-family:'IBM Plex Mono',monospace;font-size:10px;letter-spacing:.05em;text-transform:uppercase;
 padding:5px 10px;border-radius:999px;border:1px solid var(--line);color:var(--dim);cursor:pointer;background:rgba(18,22,27,.8)}
.chip .dot{display:inline-block;width:7px;height:7px;border-radius:50%;margin-right:5px;vertical-align:1px}
.chip.on{color:var(--bone);border-color:var(--steel)}
.count{font-family:'IBM Plex Mono',monospace;font-size:10px;color:var(--steel);margin-top:6px}
.voicebar{position:fixed;bottom:10px;left:0;right:0;z-index:1000;text-align:center;pointer-events:none}
.voicebar *{pointer-events:auto}
.vbtn{background:rgba(18,22,27,.9);border:1px solid var(--line);color:var(--bone);font-family:'IBM Plex Mono',monospace;
 font-size:10px;letter-spacing:.08em;text-transform:uppercase;padding:8px 14px;border-radius:999px;cursor:pointer}
.vbtn.on{border-color:var(--cold);color:var(--cold);box-shadow:0 0 12px rgba(111,168,220,.35)}
.voicesel{margin-left:6px;background:rgba(18,22,27,.9);border:1px solid var(--line);color:var(--bone);
 font-family:'IBM Plex Mono',monospace;font-size:10px;padding:7px 6px;border-radius:8px;max-width:46vw}
.voicestat{font-family:'IBM Plex Mono',monospace;font-size:9px;color:var(--steel);margin:5px 0 0;min-height:12px}
/* popup */
.leaflet-popup-content-wrapper{background:var(--char);color:var(--bone);border:1px solid var(--line);border-radius:8px}
.leaflet-popup-tip{background:var(--char)}
.leaflet-popup-content{margin:12px 14px;font-family:'IBM Plex Sans',sans-serif;font-size:13px;line-height:1.5;max-width:270px}
.pcat{font-family:'IBM Plex Mono',monospace;font-size:9px;letter-spacing:.14em;text-transform:uppercase}
.ptitle{font-weight:600;font-size:14px;margin:4px 0}
.pmeta{font-family:'IBM Plex Mono',monospace;font-size:10px;color:var(--dim)}
.pxav{font-family:'Newsreader',serif;font-style:italic;font-size:13px;color:var(--bone);margin:8px 0;border-left:2px solid var(--line);padding-left:8px}
.phelp{margin:6px 0 0;padding:0;list-style:none}
.phelp li{padding:4px 0;border-top:1px solid rgba(36,42,49,.7);font-size:12px}
.phelp .hn{font-weight:600;color:var(--cold)}
.phelp .hw{color:var(--dim)}
.phelp .hc{font-family:'IBM Plex Mono',monospace;font-size:10px;color:var(--bone)}
.pem{margin-top:8px;font-family:'IBM Plex Mono',monospace;font-size:10px;color:var(--bone);background:rgba(214,69,51,.12);border:1px solid rgba(214,69,51,.4);border-radius:6px;padding:6px 8px}
</style></head><body>
<div id="map"></div>
<div class="panel">
  <p class="eyebrow">Ava&#8217;s twin &#183; the one that goes deep</p>
  <h1>Xavier</h1>
  <p class="intro" id="intro"></p>
  <div class="chips" id="chips"></div>
  <p class="count" id="count">Waking the map&#8230;</p>
</div>
<div class="voicebar">
  <button id="voicebtn" class="vbtn" type="button">Hear Xavier</button><select id="voicesel" class="voicesel" aria-label="Choose voice"></select>
  <p class="voicestat" id="voicestat"></p>
</div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.js"></script>
<script>
var INTRO="I am Xavier. Ava is my twin: she keeps the lamp, I walk into the dark and mark where it hurts. Every mark I make carries a way to help.";
document.getElementById('intro').textContent=INTRO;
var COLORS={'Atrocity':'#D64533','Major Crime':'#E08A3C','Disaster':'#4A90D9','Drought':'#D9B44A','Famine':'#A97BE0'};
var map=L.map('map',{zoomControl:false,worldCopyJump:true}).setView([18,10],2);
L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
 {attribution:'&copy; OpenStreetMap &copy; CARTO',subdomains:'abcd',maxZoom:12}).addTo(map);
var groups={},active={};
Object.keys(COLORS).forEach(function(c){groups[c]=L.layerGroup().addTo(map);active[c]=true;});
function esc(s){return (s==null?'':String(s)).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
function popupHtml(e){
 var col=COLORS[e.category]||'#888';
 var h='<div class="pcat" style="color:'+col+'">'+esc(e.category)+' &#183; severity '+e.severity+' of 5</div>'
  +'<div class="ptitle">'+esc(e.title)+'</div>'
  +'<div class="pmeta">'+esc(e.place||e.country||'location approximate')
  +(e.precision==='exact'?' &#183; exact coordinates':(e.precision==='city'?' &#183; pinned to city':' &#183; pinned to country center'))
  +' &#183; '+esc(e.source)+'</div>';
 if(e.xavier)h+='<div class="pxav">'+esc(e.xavier)+'</div>';
 if(e.help&&e.help.length){
  h+='<ul class="phelp">';
  e.help.forEach(function(x){h+='<li><span class="hn">'+esc(x.name)+'</span> <span class="hw">'+esc(x.what)+'</span><br><span class="hc">'+esc(x.contact)+'</span></li>';});
  h+='</ul>';
 }
 h+='<div class="pem">EMERGENCY: '+esc(e.emergency)+'</div>';
 if(e.url)h+='<div class="pmeta" style="margin-top:6px"><a href="'+esc(e.url)+'" target="_blank" rel="noopener" style="color:#6FA8DC">source report</a></div>';
 return h;
}
function speakEvent(e){
 var s=e.category+'. '+e.title+'. '+(e.place||e.country||'')+'. '+(e.xavier||'');
 if(e.help&&e.help.length)s+=' Help can come from '+e.help.map(function(x){return x.name;}).join(', ')+'.';
 s+=' Emergency number: '+e.emergency+'.';
 speak(s);
}
function render(events){
 Object.keys(groups).forEach(function(c){groups[c].clearLayers();});
 var n=0;
 events.forEach(function(e){
  var col=COLORS[e.category];if(!col)return;
  var m=L.circleMarker([e.lat,e.lon],{
   radius:4+e.severity*1.6,color:col,weight:1.5,fillColor:col,
   fillOpacity:e.precision==='exact'?0.75:0.45,opacity:0.9});
  m.bindPopup(popupHtml(e),{maxWidth:290});
  m.on('popupopen',function(){speakEvent(e);});
  groups[e.category].addLayer(m);n++;
 });
 document.getElementById('count').textContent=n+' marks on the map &#183; last 7 days';
 document.getElementById('count').innerHTML=n+' marks on the map &#183; last 7 days';
}
/* chips */
(function(){
 var wrap=document.getElementById('chips');
 Object.keys(COLORS).forEach(function(c){
  var b=document.createElement('span');b.className='chip on';
  b.innerHTML='<span class="dot" style="background:'+COLORS[c]+'"></span>'+c;
  b.addEventListener('click',function(){
   active[c]=!active[c];
   b.classList.toggle('on',active[c]);
   if(active[c])map.addLayer(groups[c]);else map.removeLayer(groups[c]);
  });
  wrap.appendChild(b);
 });
})();
async function load(){
 try{
  var r=await fetch('/api/events');var data=await r.json();
  if(Array.isArray(data))render(data);
 }catch(err){document.getElementById('count').textContent='Could not reach Xavier just now.';}
}
/* ---- voice (mobile-safe: first utterance starts inside the tap) ---- */
var voiceOn=false,xVoice=null,chunkQ=[],curU=null;
function stat(t){var el=document.getElementById('voicestat');if(el)el.textContent=t||'';}
function chunksOf(text){return (text.match(/[^.!?]+[.!?]*/g)||[text]).map(function(s){return s.trim();}).filter(Boolean);}
function populateVoices(){
 if(!('speechSynthesis' in window))return;
 var vs=speechSynthesis.getVoices();
 var sel=document.getElementById('voicesel');
 var keep=sel.value;
 sel.innerHTML='';
 vs.forEach(function(v,i){var o=document.createElement('option');o.value=String(i);o.textContent=v.name+' ('+v.lang+')';sel.appendChild(o);});
 if(!vs.length){stat('No voices found yet. Enable a text-to-speech engine or try Chrome.');xVoice=null;return;}
 var best=vs.findIndex(function(v){return /male|daniel|david|george|james|alex|fred|rishi/i.test(v.name)&&!/female/i.test(v.name)&&/^en/i.test(v.lang);});
 if(best<0)best=vs.findIndex(function(v){return /^en/i.test(v.lang);});
 if(best<0)best=0;
 var idx=(keep!==''&&Number(keep)<vs.length)?Number(keep):best;
 sel.value=String(idx);
 xVoice=vs[idx]||null;
}
if('speechSynthesis' in window){
 speechSynthesis.onvoiceschanged=populateVoices;
 populateVoices();setTimeout(populateVoices,500);setTimeout(populateVoices,2000);
 setInterval(function(){if(speechSynthesis.speaking&&!speechSynthesis.paused){try{speechSynthesis.resume();}catch(err){}}},5000);
}
function stopSpeak(){chunkQ=[];if('speechSynthesis' in window)speechSynthesis.cancel();stat('');}
function speakNext(){
 if(!chunkQ.length){stat('');return;}
 curU=new SpeechSynthesisUtterance(chunkQ.shift());
 if(xVoice){curU.voice=xVoice;curU.lang=xVoice.lang;}else{curU.lang='en-US';}
 curU.pitch=0.85;curU.rate=0.98;curU.volume=1;
 curU.onstart=function(){stat('Xavier is speaking');};
 curU.onend=speakNext;
 curU.onerror=function(e){stat('Voice error: '+((e&&e.error)||'unknown')+'. Try another voice.');speakNext();};
 speechSynthesis.speak(curU);
}
function speak(text){
 if(!voiceOn||!text||!('speechSynthesis' in window))return;
 chunkQ=chunksOf(text);
 if(speechSynthesis.speaking||speechSynthesis.pending){speechSynthesis.cancel();setTimeout(speakNext,200);}
 else{speakNext();}
}
document.getElementById('voicesel').addEventListener('change',function(){
 if(!('speechSynthesis' in window))return;
 var vs=speechSynthesis.getVoices();
 xVoice=vs[Number(this.value)]||null;
 if(voiceOn){chunkQ=chunksOf('Now I sound like this.');speechSynthesis.cancel();setTimeout(speakNext,200);}
});
document.getElementById('voicebtn').addEventListener('click',function(){
 voiceOn=!voiceOn;
 this.classList.toggle('on',voiceOn);
 this.textContent=voiceOn?'Voice on':'Voice off';
 if(!voiceOn){stopSpeak();return;}
 if(!('speechSynthesis' in window)){stat('This browser cannot speak. Open this page in Chrome.');return;}
 if(location.protocol==='http:'&&location.hostname!=='localhost'&&location.hostname!=='127.0.0.1'){
  stat('Voice needs a secure page. Open the https link of your deployed site.');
 }
 populateVoices();
 chunkQ=chunksOf(INTRO);
 speechSynthesis.cancel();
 speakNext();
});
load();
setInterval(load,300000);
fetch('/api/ingest').then(function(){load();}).catch(function(){});
</script>
</body></html>"""


def main():
    init_db()
    threading.Thread(target=background_ingest, daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Xavier is watching on http://0.0.0.0:{PORT}  (Ctrl-C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nThe marks remain. Someone should answer them.")


if __name__ == "__main__":
    main()
