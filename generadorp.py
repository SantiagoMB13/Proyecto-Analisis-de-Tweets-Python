import json
import networkx as nx
import os
import sys
import argparse
import time
from datetime import datetime
import bz2
from itertools import combinations
from mpi4py import MPI

def get_tweets(path, fecha_inicial, fecha_final, hashtags, size, rank):
    tweets = []
    cont = -1
    for root, dirs, files in os.walk(path):
        for file in files:
                cont = cont + 1
                if (file.endswith(".json.bz2") and cont % size == rank):
                        file_path = os.path.join(root, file)
                        with bz2.BZ2File(file_path, "r") as f: #r
                            try:
                                for line in f:
                                    if line.strip():
                                        tweet = json.loads(line)
                                        if "created_at" in tweet:
                                            tweet_created_at = datetime.strptime(tweet["created_at"], "%a %b %d %H:%M:%S %z %Y")
                                            if fecha_inicial.date()<=tweet_created_at.date()<=fecha_final.date():
                                              if hashtags:
                                                if tweet["entities"]["hashtags"]:
                                                    hashtexts = [hashtag["text"].lower() for hashtag in tweet["entities"]["hashtags"]]
                                                    hashtextsn = [palabra.lower() for palabra in hashtexts]
                                                    added = 0
                                                    if (any(item in hashtextsn for item in hashtags) and added==0): #Para que no se guarde varias veces el mismo tweet
                                                        tweets.append(tweet)
                                                        added = 1
                                              else:
                                                tweets.append(tweet)
                            except UnicodeDecodeError:
                                print(f"Error de codificación en el archivo: {file_path}")
                            except json.decoder.JSONDecodeError as e:
                                print(f"Error de JSON en el archivo: {file_path}")
                                print(e)
    return tweets

# Crear los grafos y JSON

def agrupar_grafosrtm(tweets):
    grafo = nx.DiGraph()  # Utilizamos un grafo dirigido para representar retweets
    check = {}
    for parts in tweets:
      for item in parts:
        pair = f"{item['origin']}to{item['destination']}"
        if pair not in check:
            check[pair] = 1
            if item['origin'] not in grafo:
                grafo.add_node(item['origin'])
            if item['destination'] not in grafo:
                grafo.add_node(item['destination'])
            grafo.add_edge(item['origin'], item['destination'], weight=item['weight'])
        else: 
           grafo[item['origin']][item['destination']]["weight"] += item['weight'] 
    return grafo

def agrupar_rts(tweets):
    result = []
    check = {}
    index = 0
    for parts in tweets:
      for rt in parts:
        key = rt['username']
        if key not in check:
            check[key] = index
            index += 1
            result.append(rt)
        else:
            for tweet in rt['tweets']:
                if tweet not in result[check[key]]['tweets']:
                     result[check[key]]['tweets'][tweet] = rt['tweets'][tweet]
                     result[check[key]]['receivedRetweets'] += len(rt['tweets'][tweet]['retweetedBy'])
                else:
                    basel = len(result[check[key]]['tweets'][tweet]['retweetedBy'])
                    baseset = set(result[check[key]]['tweets'][tweet]['retweetedBy'])
                    newset = set(rt['tweets'][tweet]['retweetedBy'])
                    combinationl = list(baseset | newset)
                    result[check[key]]['tweets'][tweet]['retweetedBy'] = combinationl
                    offset = len(combinationl) - basel
                    result[check[key]]['receivedRetweets'] += offset
    sorted_list = sorted(result, key=lambda x: x['receivedRetweets'], reverse=True)
    result2 = {'retweets': sorted_list}
    return result2

def agrupar_ment(tweets):
    result = []
    check = {}
    index = 0
    for parts in tweets:
      for men in parts:
        key = men['username']
        if key not in check:
            check[key] = {'index': index, 'mentions': {}}
            index += 1
            result.append(men)
            count = 0
            for menti in result[check[key]['index']]['mentions']:
                check[key]['mentions'][menti['mentionBy']] = count
                count += 1
        else:
            for tweet in men['mentions']:
                if tweet['mentionBy'] not in check[key]['mentions']:
                     check[key]['mentions'][tweet['mentionBy']] = len(check[key]['mentions'])
                     result[check[key]['index']]['mentions'].append(tweet)
                     result[check[key]['index']]['receivedMentions'] += len(tweet['tweets'])
                else:
                    result[check[key]['index']]['mentions'][check[key]['mentions'][tweet['mentionBy']]]['tweets'].extend(tweet['tweets'])
                    result[check[key]['index']]['receivedMentions'] += len(tweet['tweets'])
    sorted_list = sorted(result, key=lambda x: x['receivedMentions'], reverse=True)
    result2 = {'retweets': sorted_list}
    return result2

def crear_grafo_retweets(tweets):
    retweets = {}
    guide = {}
    rts = []
    index = 0
    for tweet in tweets:
        if 'user' in tweet:
            user_screen_name = tweet["user"]["screen_name"]
            if "retweeted_status" in tweet and user_screen_name != 'null':
              original_tweet = tweet["retweeted_status"]
              original_user_screen_name = original_tweet["user"]["screen_name"]
              if original_user_screen_name != 'null':
                if user_screen_name not in retweets:
                    retweets[user_screen_name] = {}
                    guide[user_screen_name]={}
                if original_user_screen_name not in retweets[user_screen_name]:
                    retweets[user_screen_name][original_user_screen_name] = {'origin': user_screen_name, 'destination': original_user_screen_name, 'weight': 1}
                    rts.append(retweets[user_screen_name][original_user_screen_name])
                    guide[user_screen_name][original_user_screen_name] = index
                    index = index + 1
                else:
                    retweets[user_screen_name][original_user_screen_name]['weight'] += 1
                    rts[guide[user_screen_name][original_user_screen_name]] = retweets[user_screen_name][original_user_screen_name]

    return rts


def crear_json_retweets(tweets):
    result = {}
    elements = []
    guide = {}
    ind = 0
    for tweet in tweets:
        if 'user' in tweet:
            user_screen_name = tweet["user"]["screen_name"]

            if "retweeted_status" in tweet and user_screen_name != 'null':
              original_tweet = tweet["retweeted_status"]
              original_user_screen_name = original_tweet["user"]["screen_name"]
              if original_user_screen_name != 'null':
                if original_user_screen_name not in result:
                    result[original_user_screen_name] = {
                        'username' : original_user_screen_name, 
                        "receivedRetweets": 0,
                        "tweets": {}
                    }
                    guide[original_user_screen_name] = ind
                    ind = ind + 1
                    elements.append(result[original_user_screen_name])
                original_tweet_id = "tweetId: " + original_tweet["id_str"]

                if original_tweet_id not in result[original_user_screen_name]["tweets"]:
                    result[original_user_screen_name]["tweets"][original_tweet_id] = {
                        "retweetedBy": []
                    }
                    elements[guide[original_user_screen_name]] = result[original_user_screen_name] 
                if user_screen_name not in result[original_user_screen_name]["tweets"][original_tweet_id]["retweetedBy"]:
                    result[original_user_screen_name]["tweets"][original_tweet_id]["retweetedBy"].append(user_screen_name)
                    result[original_user_screen_name]["receivedRetweets"] += 1
                    elements[guide[original_user_screen_name]] = result[original_user_screen_name] 
    # Ordenar el JSON por número total de retweets de mayor a menor
    return elements



def crear_json_menciones(tweets):
    result = {}
    elements = []
    guide = {}
    ind = 0
    for tweet in tweets:
        if 'user' in tweet:
          if "retweeted_status" not in tweet and tweet["user"]["screen_name"] != 'null':  # Verificar que no sea un retweet
                user_screen_name = tweet["user"]["screen_name"]
                mentioned_users = [mencion["screen_name"] for mencion in tweet.get("entities", {}).get("user_mentions", [])]
                repeats = {}
                for mentioned_user in mentioned_users:
                  if mentioned_user not in repeats and mentioned_user != 'null': 
                    repeats[mentioned_user] = 1
                    if mentioned_user not in result:
                        result[mentioned_user] = {
                        "username": mentioned_user,
                        "receivedMentions": 0,
                        "mentions": []
                        }
                        guide[mentioned_user] = {'index': ind, 'mentioners': {}}
                        ind = ind + 1
                        elements.append(result[mentioned_user])
                    if user_screen_name not in guide[mentioned_user]['mentioners']:
                        result[mentioned_user]['mentions'].append({
                            "mentionBy": user_screen_name,
                            "tweets": []
                        })
                        guide[mentioned_user]['mentioners'][user_screen_name] = len(result[mentioned_user]['mentions']) - 1 
                        result[mentioned_user]["mentions"][guide[mentioned_user]['mentioners'][user_screen_name]]["tweets"].append(tweet["id_str"])
                    else:
                        result[mentioned_user]["mentions"][guide[mentioned_user]['mentioners'][user_screen_name]]["tweets"].append(tweet["id_str"])
                    result[mentioned_user]["receivedMentions"] += 1
                    elements[guide[mentioned_user]['index']] = result[mentioned_user] 
    return elements

def crear_grafo_menciones(tweets):
    mentionss = {}
    guide = {}
    rts = []
    index = 0
    for tweet in tweets:
        if 'user' in tweet:
          if "retweeted_status" not in tweet and tweet["user"]["screen_name"] != 'null':  # Verificar que no sea un retweet
            user_screen_name = tweet["user"]["screen_name"]
            mentioned_users = [mencion["screen_name"] for mencion in tweet.get("entities", {}).get("user_mentions", [])]
            repeats = {}
            if user_screen_name not in mentionss:
                mentionss[user_screen_name] = {}
                guide[user_screen_name] = {}

            for mentioned_user in mentioned_users:
              if mentioned_user not in repeats and mentioned_user != 'null': 
                repeats[mentioned_user] = 1
                if mentioned_user not in mentionss[user_screen_name]:
                    mentionss[user_screen_name][mentioned_user] = {'origin': user_screen_name, 'destination': mentioned_user, 'weight': 1}
                    rts.append(mentionss[user_screen_name][mentioned_user])
                    guide[user_screen_name][mentioned_user] = index
                    index += 1
                else:    
                     mentionss[user_screen_name][mentioned_user]['weight'] += 1
                     rts[guide[user_screen_name][mentioned_user]] = mentionss[user_screen_name][mentioned_user]

    return rts


def crear_base_coretweets(tweets):
    retweet_dict = {} 
    for tweet in tweets:
        retweeter = tweet['user']['screen_name']
        if 'retweeted_status' in tweet and 'user' in tweet:
          author = tweet['retweeted_status']['user']['screen_name']
          if author != retweeter and author != "null" and retweeter != "null":
            # Actualizar el diccionario de retweets
            if retweeter not in retweet_dict and author:
                retweet_dict[retweeter] = []
            retweet_dict[retweeter].append(author) 
    return retweet_dict

def unir_bases_coretweets(retweet_dict):
    dict = {}
    for parts in retweet_dict:
        for item in parts:
            if item not in dict:
                dict[item] = []
                dict[item].extend(parts[item])
            else:
                for author in parts[item]:
                    if author not in dict[item]:
                        dict[item].append(author)
    return dict

def crear_json_coretweets(retweet_dict):
    result = {} 
    elements = []
    guide = {}
    ind = 0 
    for clave, lista in retweet_dict.items():
        elementos_vistos = []
        for elemento in lista:
            if elemento not in elementos_vistos:
                if elemento != clave:
                    elementos_vistos.append(elemento)
        # Almacenar el par en el diccionario de pares iguales
        combinaciones = combinations(elementos_vistos, 2)
        for combo in combinaciones:
            parautores = f"authors: {[combo[0], combo[1]]}"
            parautores2 = f"authors: {[combo[1], combo[0]]}"
            if parautores not in result and parautores2 not in result:
                result[parautores] = {
                    'authors':{'u1': combo[0], 'u2': combo[1]},
                    'totalCoretweets': 0,
                    'retweeters': [] 
                }
                result[parautores]['retweeters'].append(clave)
                result[parautores]['totalCoretweets'] += 1
                guide[parautores] = ind
                ind = ind + 1
                elements.append(result[parautores])
            elif parautores in result and parautores2 not in result:
                if clave not in result[parautores]['retweeters']:
                    result[parautores]['retweeters'].append(clave)
                    result[parautores]['totalCoretweets'] += 1
                    elements[guide[parautores]] = result[parautores] 
            elif parautores2 in result and parautores not in result:
                if clave not in result[parautores2]['retweeters']:
                    result[parautores2]['retweeters'].append(clave)
                    result[parautores2]['totalCoretweets'] += 1
                    elements[guide[parautores2]] = result[parautores2] 
    sorted_list = sorted(elements, key=lambda x: x['totalCoretweets'], reverse=True)
    result2 = {'coretweets': sorted_list}
    return result2

def crear_grafo_coretweets(retweet_dict):
    grafo = nx.Graph() 
    result = {}  
    for clave, lista in retweet_dict.items():
        elementos_vistos = []
        for elemento in lista:
            if elemento not in elementos_vistos:
                if elemento != clave:
                    elementos_vistos.append(elemento)
        # Almacenar el par en el diccionario de pares iguales
        combinaciones = combinations(elementos_vistos, 2)
        for combo in combinaciones:
            parautores = f"authors: {[combo[0], combo[1]]}"
            parautores2 = f"authors: {[combo[1], combo[0]]}"
            if parautores not in result and parautores2 not in result:
                if combo[0] not in grafo:
                    grafo.add_node(combo[0])
                if combo[1] not in grafo:
                    grafo.add_node(combo[1])
                if grafo.has_edge(combo[0], combo[1]): #En teoria no se deberia dar nunca pero por si acaso
                    grafo[combo[0]][combo[1]]["weight"] += 1
                else: 
                    grafo.add_edge(combo[0], combo[1], weight=1)
                result[parautores] = {
                    'retweeters': [] 
                }
                result[parautores]['retweeters'].append(clave)
            elif parautores in result and parautores2 not in result:
                    if clave not in result[parautores]['retweeters']:
                        result[parautores]['retweeters'].append(clave)
                        if grafo.has_edge(combo[0], combo[1]): 
                            grafo[combo[0]][combo[1]]["weight"] += 1
                        else: 
                            grafo.add_edge(combo[0], combo[1], weight=1)
            elif parautores2 in result and parautores not in result:
                    if clave not in result[parautores2]['retweeters']:
                        result[parautores2]['retweeters'].append(clave)
                        if grafo.has_edge(combo[1], combo[0]): 
                            grafo[combo[1]][combo[0]]["weight"] += 1
                        else: 
                            grafo.add_edge(combo[1], combo[0], weight=1)

    return grafo



def imprimir_resultados(grafo, salida):
    if salida.endswith(".gexf"):
        nx.write_gexf(grafo, salida)
    elif salida.endswith(".json"):
        with open(salida, "w") as f:
            json.dump(list(grafo.nodes(data=True)), f, indent=4) 

def parse_args(argv):
    parser = argparse.ArgumentParser(description="Argumentos para generador.py", add_help=False)
    parser.add_argument("-d", "--directory", default="data", help="Ruta al directorio de datos")
    parser.add_argument("-fi", "--fecha-inicial", help="Fecha inicial")
    parser.add_argument("-ff", "--fecha-final", help="Fecha final")
    parser.add_argument("-h", "--hashtags", help="Lista de hashtags")
    parser.add_argument("-grt", action="store_true", help="Crear grafo de retweets")
    parser.add_argument("-jrt", action="store_true", help="Crear JSON de retweets")
    parser.add_argument("-gm", action="store_true", help="Crear grafo de menciones")
    parser.add_argument("-jm", action="store_true", help="Crear JSON de menciones")
    parser.add_argument("-gcrt", action="store_true", help="Crear grafo de corretweets")
    parser.add_argument("-jcrt", action="store_true", help="Crear JSON de corretweets")
    args = parser.parse_args(argv)
    args = vars(args)
    if "directory" not in args:
       args["directory"] = "data"
    return args

def main(rank, comm, size):
    args = parse_args(sys.argv[1:])
    
    fecha_inicial = datetime.strptime("01-01-1990", "%d-%m-%Y")
    fecha_final =   datetime.strptime("01-01-2024", "%d-%m-%Y")

    if args["fecha_inicial"]:
        fecha_inicial = datetime.strptime(args["fecha_inicial"], "%d-%m-%y")

    if args["fecha_final"]:
        fecha_final = datetime.strptime(args["fecha_final"], "%d-%m-%y")

    hashtagsl = []
    if args["hashtags"]:
        current_directory = os.getcwd()
        route = os.path.join(current_directory, args["hashtags"])
        with open(route, 'r') as archivo:
            hashtagsl = [line.strip() for line in archivo.readlines()]
        for index in range (0,len(hashtagsl)):
            word = hashtagsl[index]
            if word.startswith("#"):
                hashtagsl[index] = word[1:]

    tweets = get_tweets(args["directory"], fecha_inicial, fecha_final, hashtagsl, size, rank)

    if args["grt"]:
        grafoprt = crear_grafo_retweets(tweets)
        grafoptrt = comm.gather(grafoprt, root=0)
        if rank == 0:
            grafort = agrupar_grafosrtm(grafoptrt)
            imprimir_resultados(grafort, "rtp.gexf")

    if args["jrt"]:
        json_retweetsp = crear_json_retweets(tweets)
        json_retweetspt = comm.gather(json_retweetsp, root=0)
        if rank == 0:
            json_retweets = agrupar_rts(json_retweetspt)
            with open("rtp.json", "w") as f:
                json.dump(json_retweets, f, indent=4)  

    if args["gm"]:
        grafopm = crear_grafo_menciones(tweets)
        grafoptm = comm.gather(grafopm, root=0)
        if rank == 0:
            grafom = agrupar_grafosrtm(grafoptm)
            imprimir_resultados(grafom, "mencionp.gexf")

    if args["jm"]: 
        json_mencionesp = crear_json_menciones(tweets)
        json_mencionespt = comm.gather(json_mencionesp, root=0)
        if rank == 0:
            json_menciones = agrupar_ment(json_mencionespt)
            with open("mencionp.json", "w") as f:
                json.dump(json_menciones, f, indent=4)

    if args["gcrt"] or args["jcrt"]:
        basecrt = crear_base_coretweets(tweets)
        allbase = comm.gather(basecrt, root=0)
        if rank == 0:
            finalcrt = unir_bases_coretweets(allbase)

    if args["gcrt"]:
        if rank == 0:
            grafocrt = crear_grafo_coretweets(finalcrt)
            imprimir_resultados(grafocrt, "corrtwp.gexf")

    if args["jcrt"]:
        if rank == 0:
            json_corretweets = crear_json_coretweets(finalcrt)
            with open("corrtwp.json", "w") as f:
                json.dump(json_corretweets, f, indent=4)
    if rank == 0:
        print(time.time() - start_time)

if __name__ == "__main__":
    start_time = time.time()
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    main(rank, comm, size)
