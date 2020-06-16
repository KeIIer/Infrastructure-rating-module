# -*- coding: utf8 -*-
import time
import requests
import googlemaps
import re
import numpy
import json
import sys
import os
from scipy.stats.mstats import gmean
import math
from googleplaces import GooglePlaces, types
from pymongo import MongoClient


# Функция, осуществляющая поиск объектов инфраструктуры с помощью Google API
# Принимает 3 аргумента
# location - место, вокруг которого производится поиск инфраструктуры
# key - ключ аккаунт Google для работы с API
# radius - радиус, в котором производится поиск
def infrastructure_finder(location, key, radius):
    gmaps = googlemaps.Client(key=key)
    google_places = GooglePlaces(key)
    geocode_result = gmaps.geocode(location, language = 'ru')

    if geocode_result != []:
        lat = re.findall(r"'lat': (\d*.\d*)", str(geocode_result))
        lng = re.findall(r"'lng': (\d*.\d*)", str(geocode_result))
        print(lat[0], lng[0])
        print(lat[1], lng[1])
        print(lat[2], lng[2])
    else:
        return [0]*9, 0, 0, 0

    query_result_education = google_places.nearby_search(
        lat_lng={'lat': lat[0], 'lng': lng[0]},
        radius=radius,
        types=[types.TYPE_SCHOOL],
        language='ru')

    query_result_health = google_places.nearby_search(
        lat_lng={'lat': lat[0], 'lng': lng[0]},
        radius=radius,
        types=[types.TYPE_HOSPITAL],
        language='ru')

    query_result_transport = google_places.nearby_search(
        lat_lng={'lat': lat[0], 'lng': lng[0]},
        radius=radius,
        types=[types.TYPE_TRANSIT_STATION],
        language='ru')

    query_result_pharmacy = google_places.nearby_search(
        lat_lng={'lat': lat[0], 'lng': lng[0]},
        radius=radius,
        types=[types.TYPE_PHARMACY],
        language='ru')

    query_result_sport = google_places.nearby_search(
        lat_lng={'lat': lat[0], 'lng': lng[0]},
        radius=radius,
        types=[types.TYPE_GYM],
        language='ru')

    query_result_care = google_places.nearby_search(
        lat_lng={'lat': lat[0], 'lng': lng[0]},
        radius=radius,
        types=[types.TYPE_HAIR_CARE],
        language='ru')

    query_result_public_catering = google_places.nearby_search(
        lat_lng={'lat': lat[0], 'lng': lng[0]},
        radius=radius,
        types=[types.TYPE_CAFE],
        language='ru')

    query_result_food = google_places.nearby_search(
        lat_lng={'lat': lat[0], 'lng': lng[0]},
        radius=radius,
        types=[types.TYPE_SHOPPING_MALL],
        language='ru')

    query_result_money = google_places.nearby_search(
        lat_lng={'lat': lat[0], 'lng': lng[0]},
        radius=radius,
        types=[types.TYPE_ATM],
        language='ru')

    query_results = numpy.array([query_result_education, query_result_health, query_result_transport,
                                 query_result_pharmacy, query_result_sport, query_result_care,
                                 query_result_public_catering, query_result_food, query_result_money])

    return query_results, lat, lng, geocode_result[0]['formatted_address']


# Функция, осуществляющая перевод координат из листа в радианы
# Принимает 1 аргумент
# var - элемент листа
def list_to_radians(var):
    new_var = math.radians(float(str(var)[2:-2]))
    return new_var


# Функция, определеяющая ближайшие объекты инфраструктуры, а так же расстояние от объекта недвижимости
# до объектов инфраструктуры для Google Maps
# Принимает 5 аргументов
# input_list - лист с объектами
# lat - широта объекта недвижимости
# lng - долгота объекта недвижимости
# key - ключ аккаунта Google для работы с API
# output_list - лист, содержащий в себе информацию об объектах инфраструктуры
def distance_determinant(input_list, lat, lng, key, output_list):
    counter = 0
    if not input_list.places:
        value = 0
        output_list = 0
        return output_list, value
    else:
        gmaps = googlemaps.Client(key=key)
        lat_new = []
        lng_new = []
        for place in input_list.places:
            lat_new.append(re.findall(r"'lat': Decimal[(]'(\d*.\d*)", str(place.geo_location)))
            lng_new.append(re.findall(r"'lng': Decimal[(]'(\d*.\d*)", str(place.geo_location)))

        for i in range(len(lat_new)):
            lat_new[i] = list_to_radians(lat_new[i])
            lng_new[i] = list_to_radians(lng_new[i])

        lat_r = math.radians(float(lat))
        lng_r = math.radians(float(lng))

        r = 6373.0
        distance_arr = []
        for i in range(len(lat_new)):
            dlat = lat_new[i] - lat_r
            dlng = lng_new[i] - lng_r
            a = math.sin(dlat / 2) ** 2 + math.cos(lat_r) * math.cos(lat_new[i]) * math.sin(dlng / 2) ** 2
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
            dist = r * c
            distance_arr.append(dist)
        min_id = distance_arr.index(min(distance_arr))

        lat_new[min_id] = math.degrees(lat_new[min_id])
        lng_new[min_id] = math.degrees(lng_new[min_id])

        distance_gmaps = gmaps.distance_matrix((lat, lng),
                                               (lat_new[min_id], lng_new[min_id]), mode='walking')

        for place in input_list.places:
            if counter == min_id:
                # print(place.name)
                output_list = place
            counter += 1

        if 'rows' in distance_gmaps:
            value = float(re.findall(r"'value': (\d*)", str(distance_gmaps['rows']))[0])
            # print(value)
        else:
            print('no data')

        return output_list, value


# Функция, осуществляющая сбор подробной информации об объекте инрфаструктуры
# Принимает 1 аргумент
# input - объект инфраструктуры
def get_info(input):
    input.get_details()
    output = input.details
    return output


# Функция, создающая и обновляющая запись в БД
# Принимает 3 аргумента
# input_list - лист с входными данными
# address - адрес объекта недвижимости
# types - список типов объектов инфраструктуры
def create_record(input_list, address, types, searching_radius):
    searching_radius = float(searching_radius)
    print(input_list['Objects'][0]['distance'])
    if input_list['Objects'][0]['id'] != 'Нет данных' and input_list['Objects'][0]['distance'] <= searching_radius:
        new_record = {'Address': address,
                      'Objects': [{'id': input_list['Objects'][0]['id'],
                                   'address': input_list['Objects'][0]['address'],
                                   'name': input_list['Objects'][0]['name'],
                                   'type': input_list['Objects'][0]['type'],
                                   'distance': input_list['Objects'][0]['distance']}
                                  ],
                      'Rating': ''}
    else:
        new_record = {'Address': address,
                      'Objects': [{'id': 'Не найдено',
                                   'address': 'Не найдено',
                                   'name': 'Не найдено',
                                   'type': types[0],
                                   'distance': 'Нет данных'}
                                  ],
                      'Rating': ''}

    collection.insert_one(new_record)

    for i in range(1, len(input_list['Objects'])):
        print(input_list['Objects'][i]['distance'])
        if input_list['Objects'][i]['id'] != 'Нет данных' and input_list['Objects'][i]['distance'] != 'Нет данных' and \
                int(input_list['Objects'][i]['distance']) <= searching_radius:
            collection.update_one({"Address": address},
                                  {'$push': {
                                      'Objects':
                                          {
                                              'id': input_list['Objects'][i]['id'],
                                              'address': input_list['Objects'][i]['address'],
                                              'name': input_list['Objects'][i]['name'],
                                              'type': input_list['Objects'][i]['type'],
                                              'distance': input_list['Objects'][i]['distance']
                                          }
                                  }})
        else:
            collection.update_one({"Address": address},
                                  {'$push': {
                                      'Objects':
                                          {
                                              'id': 'Не найдено',
                                              'address': 'Не найдено',
                                              'name': 'Не найдено',
                                              'type': types[i],
                                              'distance': 'Нет данных'
                                          }
                                  }})


# Функция, создающая и обновляющая json файл с общими собранными данными
# Принимает 2 аргумента
# input - данные, которые нужно занести в БД
# address - адрес объекта недвижимости
def create_json(input, address):
    using_path = 'C:\\Users\\Keller\\Desktop\\Диплом\\test\\Finale\\JSON'
    changed = 0
    if not os.path.exists(using_path):
        os.mkdir(using_path)

    if not os.path.isfile(using_path + '\\' + address + '.json'):
        print('creating')
        with open(using_path + '\\' + address + '.json', 'w') as outfile:
            json.dump(input, outfile, indent=4, ensure_ascii=False)
    else:
        with open(using_path + '\\' + address + '.json', 'r') as f:
            json_data_old = json.load(f)
        if json_data_old != input:
            print('not equal')
            for i in range(len(json_data_old['Objects'])):
                print(i)
                if json_data_old['Objects'][i]['distance'] != 'Нет данных' and \
                        input['Objects'][i]['distance'] != 'Нет данных':
                    if int(input['Objects'][i]['distance']) < int(json_data_old['Objects'][i]['distance']):
                        json_data_old['Objects'][i]['distance'] = input['Objects'][i]['distance']
                        json_data_old['Objects'][i]['name'] = input['Objects'][i]['name']
                        json_data_old['Objects'][i]['address'] = input['Objects'][i]['address']
                        json_data_old['Objects'][i]['id'] = input['Objects'][i]['id']
                        changed = 1
                elif json_data_old['Objects'][i]['distance'] == 'Нет данных' and \
                        input['Objects'][i]['distance'] != 'Нет данных':
                    json_data_old['Objects'][i]['distance'] = input['Objects'][i]['distance']
                    json_data_old['Objects'][i]['name'] = input['Objects'][i]['name']
                    json_data_old['Objects'][i]['address'] = input['Objects'][i]['address']
                    json_data_old['Objects'][i]['id'] = input['Objects'][i]['id']
                    changed = 1
            if changed == 1:
                with open(using_path + '\\' + address + '.json', 'w') as outfile:
                    json.dump(json_data_old, outfile, indent=4, ensure_ascii=False)
                    print('Json changed')


# Функция, определяющая координаты и верифицирующая адрес объекта недвижимости
# Принимает 2 аргумента
# location - не верифицированный адрес объекта недвижимости
# key - ключ для работы с Google Geocoder API
def get_coordinates(location, key):
    gmaps = googlemaps.Client(key=key)
    geocode_result = gmaps.geocode(location, language = 'ru')

    lat = re.findall(r"'lat': (\d*.\d*)", str(geocode_result))
    lng = re.findall(r"'lng': (\d*.\d*)", str(geocode_result))

    return lat[0], lng[0], geocode_result[0]['formatted_address']


# Функция, определяющая расстояние от объекта недвижимости до объектов инфраструктуры
# Принимает 5 аргументов
# key - ключ для работы с Google Distance Matrix API
# lat_from - широта объекта недвижимости
# lng_from - долгота объекта недвижимости
# lat_to - широта объекта инфраструктуры
# lng_to - долгота объекта инфраструктуры
def gmaps_distance_determinant(key, lat_from, lng_from, lat_to, lng_to):
    gmaps = googlemaps.Client(key=key)
    distance_gmaps = gmaps.distance_matrix((lat_from, lng_from),
                                           (lat_to, lng_to), mode='walking')
    if distance_gmaps['rows']:
        value = distance_gmaps['rows'][0]['elements'][0]['distance']['value']
        return value
    else:
        return 'Нет данных'


# Функция, с помощью которой находятся объекты инфраструктуры в Wikimapia, а так же определяющая ближайшие их них
# Принимает 5 аргументов
# wikimapia_request - шаблон запроса к Wikimapia
# googlemaps_request - шаблон запроса к Google Maps
# categories - долгота объекта недвижимости
# lat - широта объекта недвижимости
# lng - долгота объекта недвижимости
# Wikimapia_API_KEY - ключ для работы с Wikimapia API
def wikimapia_get_nearest(wikimapia_request, googlemaps_request, categories, lat, lng, Wikimapia_API_KEY):
    names, locations_lat, locations_lng, addresses, place_ids = \
                                          [0] * len(using_categories), [0] * len(using_categories), \
                                          [0] * len(using_categories), [''] * len(using_categories), \
                                          [0] * len(using_categories)

    for i in range(len(categories)):
        print('------ ' + str(i + 1) + ' ------')

        request_str = wikimapia_request.format(Wikimapia_API_KEY, categories[i], lat, lng)
        response_wikimapia = requests.get(request_str)
        data_wikimapia = json.loads(response_wikimapia.text)

        if data_wikimapia['folder']:

            names[i] = data_wikimapia['folder'][0]['name']
            locations_lat[i] = data_wikimapia['folder'][0]['location']['lat']
            locations_lng[i] = data_wikimapia['folder'][0]['location']['lon']
            print(locations_lat[i], locations_lng[i])

            if locations_lat[i] and locations_lat[i]:
                gmaps_request = googlemaps_request.format(locations_lat[i], locations_lng[i], API_KEY)
                response_gmaps = requests.get(gmaps_request)
                data_gmaps = json.loads(response_gmaps.text)

                addresses[i] = data_gmaps['results'][1]['formatted_address']
                place_ids[i] = data_gmaps['results'][1]['place_id']
            else:
                addresses[i] = 'Нет данных'
                place_ids[i] = 'Нет данных'
        else:
            print('Key limit has been reached')
        print('---------------')

    return names, addresses, place_ids, locations_lat, locations_lng


# Функция, рассчитывающая расстояние по прямой линии для отдельного объекта инфраструктуры
# Принимает 4 аргумента
# flat - широта объекта недвижимости
# flng - долгота объекта недвижимости
# lat - широта объекта инфраструктуры
# lng - долгота объекта инфраструктуры
def distance_determinant_osm(flat, flng, tlat, tlng):
    print(flat, flng, tlat, tlng)
    flat_radians = math.radians(flat)
    flng_radians = math.radians(flng)
    tlat_radians = math.radians(tlat)
    tlng_radians = math.radians(tlng)
    r = 6373.0
    dlat = tlat_radians - flat_radians
    dlng = tlng_radians - flng_radians
    a = math.sin(dlat / 2) ** 2 + math.cos(flat_radians) * math.cos(tlat_radians) * math.sin(dlng / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = r * c
    print(distance)
    return distance


# Функция, собирающая данные об инфраструктуре с сервиса Google Maps
# Принимает 1 аргумент
# address - адрес объекта недвижимости, вводится с клавиатуры
def google_parse(address):
    print('starting_google_parse')
    print(address)
    data = []
    json_data = {'Address': address,
                 'Objects': data,
                 'Rating': ''}
    closeObjects, values, info = [0] * len(using_types), numpy.zeros(len(using_types)), [0] * len(using_types)

    results, lat, lng, address = infrastructure_finder(address, API_KEY, searching_radius)

    if address == 0:
        print("Неверный адрес")
        sys.exit()

    print(address)

    for iterator in range(len(results)):
        closeObjects[iterator], values[iterator] = distance_determinant(results[iterator], lat[2], lng[2], API_KEY,
                                                                        closeObjects[iterator])

    for iterator in range(len(results)):
        if closeObjects[iterator] == 0:
            print("\n" + "Нет данных о типе: " + using_types[iterator] + "\n")
            info[iterator] = 'No data'
        else:
            # info.append(get_info(closeObjects[iterator]))  # динамически
            info[iterator] = get_info((closeObjects[iterator]))  # известна длина листа

    for i in range(len(info)):
        if info[i] != 'No data':
            data.append({'id': str(info[i]['place_id']),
                         'address': str(info[i]['formatted_address']),
                         'name': str(info[i]['name']),
                         'type': using_types[i],
                         'distance': values[i]})
        else:
            data.append({'id': 'Не найдено',
                         'address': 'Не найдено',
                         'name': 'Не найдено',
                         'type': using_types[i],
                         'distance': 'Нет данных'})

    with open('Google_' + address + '.json', 'w') as outfile:
        json.dump(json_data, outfile, indent=4, ensure_ascii=False)

    founded_document = collection.find_one({'Address': address})

    create_json(json_data, address)

    if not founded_document:
        create_record(json_data, address, using_types, searching_radius)
    elif founded_document:
        print('Была найдена соответствующая запись в базе данных')
        for i in range(len(using_types)):
            print(i, founded_document['Objects'][i]['distance'], json_data['Objects'][i]['distance'])
            if founded_document['Objects'][i]['distance'] == 'Нет данных' and \
                    json_data['Objects'][i]['distance'] != 'Нет данных' and \
                    int(json_data['Objects'][i]['distance']) <= searching_radius:
                collection.update_one({
                    'Address': address,
                    'Objects.distance': founded_document['Objects'][i]['distance']
                },
                    {
                        '$set': {
                            'Objects.$.distance': json_data['Objects'][i]['distance'],
                            'Objects.$.name': json_data['Objects'][i]['name'],
                            'Objects.$.id': json_data['Objects'][i]['id'],
                            'Objects.$.address': json_data['Objects'][i]['address']
                        },
                    })
            elif founded_document['Objects'][i]['distance'] != 'Нет данных' and \
                    json_data['Objects'][i]['distance'] != 'Нет данных':
                if isinstance(int(founded_document['Objects'][i]['distance']), int) and \
                        isinstance(int(json_data['Objects'][i]['distance']), int):
                    if int(founded_document['Objects'][i]['distance']) > int(json_data['Objects'][i]['distance']):
                        print('Updating current record')
                        collection.update_one({
                            'Address': address,
                            'Objects.type': founded_document['Objects'][i]['type'] # Last change
                        },
                            {
                                '$set': {
                                    'Objects.$.distance': json_data['Objects'][i]['distance'],
                                    'Objects.$.name': json_data['Objects'][i]['name'],
                                    'Objects.$.id': json_data['Objects'][i]['id'],
                                    'Objects.$.address': json_data['Objects'][i]['address']
                                },
                            })
    print('\n', '--------------------------------- Google Done --------------------------------------', '\n')


# Функция, собирающая данные об инфраструктуре с сервиса Wikimapia
# Принимает 5 аргументов
# address - верифицированный адрес объекта недвижимости
# API_KEY - ключ для работы с Google Maps API
# Wikimapia_API_KEY_1 - ключ для работы с Wikimapia API
# lat - широта объекта недвижимости
# lng - долгота объекта недвижимости
def wikimapia_parse(address, API_KEY, Wikimapia_API_KEY_1, lat, lng):
    print('starting_wikimapia_parse')
    wikimapia_data = []
    json_data = {'Address': address,
                 'Objects': wikimapia_data,
                 'Rating': ''}
    wikimapia_distances = [''] * len(using_categories)
    names, addresses, place_ids, obj_lats, obj_lngs = wikimapia_get_nearest(request_str, inverse_geocode_gmaps_request,
                                                                            using_categories, lat, lng,
                                                                            Wikimapia_API_KEY_1)

    for i in range(len(using_categories)):
        wikimapia_distances[i] = gmaps_distance_determinant(API_KEY, lat, lng, obj_lats[i], obj_lngs[i])

    for i in range(len(using_categories)):
        if place_ids[i] != 'Нет данных':
            wikimapia_data.append({'id': place_ids[i],
                                   'address': addresses[i],
                                   'name': names[i],
                                   'type': using_types[i],
                                   'distance': wikimapia_distances[i]})
        else:
            wikimapia_data.append({'id': 'Нет данных',
                                   'address': 'Нет данных',
                                   'name': 'Нет данных',
                                   'type': using_types[i],
                                   'distance': 'Нет данных'})

    with open('Wikimapia_' + address + '.json', 'w') as outfile:
        json.dump(json_data, outfile, indent=4, ensure_ascii=False)

    founded_document = collection.find_one({'Address': address})

    create_json(json_data, address)

    if not founded_document:
        create_record(json_data, address, using_types)
    elif founded_document:
        print('Была найдена соответствующая запись в базе данных')
        for i in range(len(using_types)):
            if founded_document['Objects'][i]['distance'] == 'Нет данных' and \
                    json_data['Objects'][i]['distance'] != 'Нет данных' and \
                    int(json_data['Objects'][i]['distance']) <= searching_radius:
                print('Изменение записи номер {} в базе данных'.format(i))
                collection.update_one({
                    'Address': address,
                    'Objects.distance': founded_document['Objects'][i]['distance']
                },
                    {
                        '$set': {
                            'Objects.$.distance': json_data['Objects'][i]['distance'],
                            'Objects.$.name': json_data['Objects'][i]['name'],
                            'Objects.$.id': json_data['Objects'][i]['id'],
                            'Objects.$.address': json_data['Objects'][i]['address']
                        },
                    })
            elif founded_document['Objects'][i]['distance'] != 'Нет данных' and \
                    json_data['Objects'][i]['distance'] != 'Нет данных':
                if isinstance(int(founded_document['Objects'][i]['distance']), int) and \
                        isinstance(int(json_data['Objects'][i]['distance']), int):
                    if int(founded_document['Objects'][i]['distance']) > int(json_data['Objects'][i]['distance']):
                        print('Изменение записи номер {} в базе данных'.format(i))
                        collection.update_one({
                            'Address': address,
                            'Objects.type': founded_document['Objects'][i]['type']  # Last change
                        },
                            {
                                '$set': {
                                    'Objects.$.distance': json_data['Objects'][i]['distance'],
                                    'Objects.$.name': json_data['Objects'][i]['name'],
                                    'Objects.$.id': json_data['Objects'][i]['id'],
                                    'Objects.$.address': json_data['Objects'][i]['address']
                                },
                            })
    print('\n', '--------------------------------- Wikimapia done --------------------------------------', '\n')


# Функция, собирающая данные об инфраструктуре с сервиса Wikimapia
# Принимает 4 аргумента
# address - верифицированный адрес объекта недвижимости
# API_KEY - ключ для работы с Google Maps API
# lat - широта объекта недвижимости
# lng - долгота объекта недвижимости
def openstreetmap_parse(address, API_KEY, lat, lng, searching_radius):
    print('starting_openstreetmap_parse')
    using_types_names = ['Школа', 'Больница', 'Автобусная остановка', 'Аптека', 'Фитнес центр', 'Салон красоты', 'Кафе',
                        'Продуктовый магазин', 'Банкомат']
    request_types = ['amenity=school', 'amenity=clinic', 'highway=bus_stop', 'amenity=pharmacy',
                     'leisure=fitness_centre', 'shop=beauty', 'amenity=cafe', 'shop=supermarket', 'amenity=atm']

    responses, results, min_distances = [0] * len(using_types), [0] * len(using_types), [0] * len(using_types)
    distances = []
    min_ids = []

    OSM_addresses, OSM_place_ids, OSM_names = [0] * len(using_types), [0] * len(using_types), [0] * len(using_types)

    gmaps_distances = [0] * len(using_types)

    overpass_query = "[out:json];node[{}](around:{},{},{});out;"
    overpass_url = "http://overpass-api.de/api/interpreter"

    googlemaps_request = \
        'https://maps.googleapis.com/maps/api/geocode/json?latlng={},{}&key={}&language=ru'

    OSM_data = []
    OSM_json_data = {'Address': address,
                     'Objects': OSM_data,
                     'Rating': ''}

    print('Парсинг...')
    for i in range(len(using_types)):
        responses[i] = requests.get(overpass_url,
                                    params={'data': overpass_query.format(request_types[i], str(searching_radius),
                                                                          lat, lng)})
        if str(responses[i]) == '<Response [200]>':
            data = responses[i].json()
            if data['elements']:
                results[i] = data['elements']
                # print(results[i])
        elif str(responses[i]) == '<Response [504]>':
            print('Server is load too much, retrying...')
            i = i - 1
            time.sleep(1)
        time.sleep(1)
    print('Парсинг прошел успешно')

    for i in range(len(results)):
        print('----------------- ' + using_types[i] + ' -----------------')
        distances.clear()
        if results[i] != 0:
            for j in range(len(results[i])):
                print('----------------- ' + str(j) + ' -----------------')
                print(results[i][j]['lat'])
                print(results[i][j]['lon'])
                distance = distance_determinant_osm(float(lat), float(lng),
                                                    float(results[i][j]['lat']), float(results[i][j]['lon']))
                distances.append(distance)

            min_distances[i] = min(distances)
            min_ids.append(distances.index(min(distances)))

            OSM_gmaps_request = googlemaps_request.format(results[i][min_ids[i]]['lat'], results[i][min_ids[i]]['lon'],
                                                          API_KEY)
            OSM_response_gmaps = requests.get(OSM_gmaps_request)
            OSM_data_gmaps = json.loads(OSM_response_gmaps.text)
            OSM_addresses[i] = OSM_data_gmaps['results'][1]['formatted_address']
            OSM_place_ids[i] = OSM_data_gmaps['results'][1]['place_id']

            gmaps_distances[i] = gmaps_distance_determinant(API_KEY,
                                                            lat, lng,
                                                            results[i][min_ids[i]]['lat'],
                                                            results[i][min_ids[i]]['lon'])

            if 'name' in results[i][min_ids[i]]['tags']:
                OSM_names[i] = results[i][min_ids[i]]['tags']['name']
            else:
                OSM_names[i] = using_types_names[i]

            print('Minimal distance for ' + using_types[i] + ': ' + str(min_distances[i]) + ' with id: ' + str(
                min_ids[i]))
        else:
            print('No data')
            min_ids.append(0)
            OSM_addresses[i] = 'Нет данных'
            OSM_place_ids[i] = 'Нет данных'
            OSM_names[i] = 'Нет данных'
            gmaps_distances[i] = 'Нет данных'

    for i in range(len(using_types)):
        if OSM_place_ids[i] != 'Нет данных':
            OSM_data.append({'id': OSM_place_ids[i],
                             'address': str(OSM_addresses[i]),
                             'name': OSM_names[i],
                             'type': using_types[i],
                             'distance': gmaps_distances[i]})
        else:
            OSM_data.append({'id': 'Нет данных',
                             'address': 'Нет данных',
                             'name': 'Нет данных',
                             'type': using_types[i],
                             'distance': 'Нет данных'})

    founded_document = collection.find_one({'Address': address})

    with open('OSM_' + address + '.json', 'w') as outfile:
        json.dump(OSM_json_data, outfile, indent=4, ensure_ascii=False)

    for i in range(len(OSM_json_data['Objects'])):
        print(OSM_json_data['Objects'][i]['address'])

    json_data = OSM_json_data
    '''with open(OSM_address + '.json', 'r') as f:
        json_data = json.load(f)'''

    create_json(json_data, address)

    if not founded_document:
        create_record(json_data, address, using_types)
    elif founded_document:
        print('Была найдена соответствующая запись в базе данных')
        for i in range(len(using_types)):
            if founded_document['Objects'][i]['distance'] == 'Нет данных' and \
                    json_data['Objects'][i]['distance'] != 'Нет данных' and \
                    int(json_data['Objects'][i]['distance']) <= searching_radius:
                print('Изменение записи номер {} в базе данных'.format(i))
                collection.update_one({
                    'Address': address,
                    'Objects.distance': founded_document['Objects'][i]['distance']
                },
                    {
                        '$set': {
                            'Objects.$.distance': json_data['Objects'][i]['distance'],
                            'Objects.$.name': json_data['Objects'][i]['name'],
                            'Objects.$.id': json_data['Objects'][i]['id'],
                            'Objects.$.address': json_data['Objects'][i]['address']
                        },
                    })
            elif founded_document['Objects'][i]['distance'] != 'Нет данных' and \
                    json_data['Objects'][i]['distance'] != 'Нет данных':
                if isinstance(int(founded_document['Objects'][i]['distance']), int) and \
                        isinstance(int(json_data['Objects'][i]['distance']), int):
                    if int(founded_document['Objects'][i]['distance']) > int(json_data['Objects'][i]['distance']):
                        print('Изменение записи номер {} в базе данных'.format(i))
                        collection.update_one({
                            'Address': address,
                            'Objects.type': founded_document['Objects'][i]['type']
                        },
                            {
                                '$set': {
                                    'Objects.$.distance': json_data['Objects'][i]['distance'],
                                    'Objects.$.name': json_data['Objects'][i]['name'],
                                    'Objects.$.id': json_data['Objects'][i]['id'],
                                    'Objects.$.address': json_data['Objects'][i]['address']
                                },
                            })
    print('\n', '--------------------------------- OpenStreetMap Done --------------------------------------', '\n')


# Функция для расчета среднего арифмитического
# Принимает 1 аргумент
# lst - лист со значениями
def a_mean(lst):
    return sum(lst) / len(lst)


# Функция для расчета оценки
# Принимает 1 аргумент
# lst - лист со значениями
def rate(lst, searching_radius):
    new_list = []
    counter = 0
    for i in range(len(lst)):
        if lst[i] < searching_radius:
            new_list.append(lst[i])
        elif lst[i] == searching_radius:
            new_list.append(lst[i])
            counter += -0.15
        else:
            new_list.append(0)
            counter += 1.0
    if new_list:
        print(new_list)
        print(10 - a_mean(new_list) / 100, counter)
        a_rate = 10 - a_mean(new_list)/100 - counter
        if a_rate < 0:
            a_rate = 0
    else:
        a_rate = 0
    return a_rate


# Функция для добавления рассчитаной оценки в БД
# Принимает 1 аргумент
# address - верифицированный адрес объекта недвижимости
def add_rate(address):
    collection = db.Immovable_object
    distances_rating = numpy.zeros(9)

    founded_document = collection.find_one({'Address': address})

    for i in range(len(founded_document['Objects'])):
        if founded_document['Objects'][i]['distance'] != 'Нет данных':
            distances_rating[i] = founded_document['Objects'][i]['distance']
        else:
            distances_rating[i] = 1500

    rating = rate(distances_rating, searching_radius)

    rating = round(rating, 2)

    if founded_document['Rating'] != rating:
        print('Updating rate')
        collection.update_one({'Address': address},
                              {
                                  '$set':
                                      {
                                          'Rating': rating
                                      }
                              })


# ===================================================== Main ========================================================= #

# ===================================================== Google ======================================================= #
client = MongoClient('mongodb+srv://dbKeller:dbKeller@cluster0-jkn6r.mongodb.net/test?retryWrites=true&w=majority')
db = client.get_database('Immovables')
collection = db.Immovable_object
API_KEY = 'AIzaSyDF4aF0P-Vcdy7-DytX0XDkufr6gXojCQA'  # Ключ для работы с Google API
objects = numpy.zeros(5)
distance = 0
iterator = 0
address = input('Введите адрес: ')
searching_radius = 750
using_types = ['Образование', 'Лечение', 'Транспорт',
               'Аптеки', 'Спорт', 'Уход', 'Общепит',
               'Продукты питания', 'Деньги']

# =================================================== Wikimapia ====================================================== #
Wikimapia_API_KEY_1 = '3601B1B5-44D1ED18-798B3F8D-BA13406A-CC91976E-8F2B8C45-8CAEA92B-1AC1D876'

inverse_geocode_gmaps_request = \
    'https://maps.googleapis.com/maps/api/geocode/json?latlng={},{}&key={}&language=ru'

using_categories = ['школа', 'больница', 'автобусная+остановка', 'аптека', 'спортивный/тренажёрный+зал',
                    'парикмахерская', 'кафе', 'продуктовый+магазин', 'банкомат']

request_str = \
    'http://api.wikimapia.org/?function=search&key={}&q={}&lat={}6&lon={}&format=json&language=ru&count=5'

# =================================================== OpenStreetMap ================================================== #
using_types_names = ['Школа', 'Больница', 'Автобусная остановка', 'Аптека', 'Фитнес центр', 'Салон красоты', 'Кафе',
                     'Продуктовый магазин', 'Банкомат']

request_types = ['amenity=school', 'amenity=clinic', 'highway=bus_stop', 'amenity=pharmacy',
                 'leisure=fitness_centre', 'shop=beauty', 'amenity=cafe', 'shop=supermarket', 'amenity=atm']

# =================================================== Вызов функций ================================================== #

lat, lng, address = get_coordinates(address, API_KEY)

google_parse(address)

wikimapia_parse(address, API_KEY, Wikimapia_API_KEY_1, lat, lng)

openstreetmap_parse(address, API_KEY, lat, lng, searching_radius)

add_rate(address)
