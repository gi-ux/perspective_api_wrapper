import numpy as np
import pandas as pd
from concurrent.futures import wait as futures_wait
from concurrent.futures.process import ProcessPoolExecutor
import json
import time
import tqdm
from googleapiclient import discovery

workers = 4 #NUMERO DI CREDENZIALI CHE HAI

def parallel_execution(texts: list):
    futures = []
    results = pd.DataFrame()
    executor = ProcessPoolExecutor(max_workers=workers)
    sublist = np.array_split(texts, workers)
    count = 0
    keys = get_credentials()
    for sc in sublist:
        futures.append(executor.submit(score, keys[count], sc, count))
        count = count + 1
    futures_wait(futures)
    for fut in futures:
        results = pd.concat([results, fut.result()], axis=0)
    results.reset_index(drop=True, inplace=True)
    return results


def get_credentials():
    jsonFile = open(r'C:\Users\gianl\Desktop\Gi\Supsi\Vaccines_Discussion_Italy\Italian\Files\auth.json', 'r')
    config = json.load(jsonFile)
    jsonFile.close()
    keys = []

    for i in range(len(config)):
        keys.append(config[f"api_key_{i}"])
    return keys


def score(API_KEY: str, text: list, count: int):
    print(f"Worker {count} started!")

    content = []
    SEXUALLY_EXPLICIT = []
    INSULT = []
    PROFANITY = []
    TOXICITY = []
    LIKELY_TO_REJECT = []
    THREAT = []
    IDENTITY_ATTACK = []
    SEVERE_TOXICITY = []
    scores = [
        SEVERE_TOXICITY,
        IDENTITY_ATTACK,
        SEXUALLY_EXPLICIT,
        LIKELY_TO_REJECT,
        INSULT,
        PROFANITY,
        # TOXICITY,
        THREAT
    ]

    for i in tqdm.tqdm(text):
        client = discovery.build(
            "commentanalyzer",
            "v1alpha1",
            developerKey=API_KEY,
            discoveryServiceUrl="https://commentanalyzer.googleapis.com/$discovery/rest?version=v1alpha1",
            static_discovery=False,
        )

        analyze_request = {
            'comment': {'text': i},
            'requestedAttributes': {

                'SEVERE_TOXICITY': {},
                'IDENTITY_ATTACK': {},
                'SEXUALLY_EXPLICIT': {},
                'LIKELY_TO_REJECT': {},
                'INSULT': {},
                'PROFANITY': {},
                # 'TOXICITY': {},
                "THREAT": {}
            }
        }
        try:
            response = client.comments().analyze(body=analyze_request).execute()
            cont = 0
            for values in response["attributeScores"]:
                # print(values)
                for score in response["attributeScores"][values]:
                    for sum_score in response["attributeScores"][values][score]:
                        if sum_score == "value":
                            # print(response["attributeScores"][values][score][sum_score])
                            scores[cont].append(response["attributeScores"][values][score][sum_score])
                            cont += 1

            content.append(i)

        except Exception as e:
            print("Error: ", e)
            print(f"On text:{i}")
        time.sleep(1)
    print(f"Shutting down worker {count}...")
    df = pd.DataFrame(list(zip(content, scores[0], scores[1], scores[2], scores[3], scores[4], scores[5], scores[6])),
                      columns=["text", "SEVERE_TOXICITY", "IDENTITY_ATTACK", "SEXUALLY_EXPLICIT", "LIKELY_TO_REJECT",
                               "INSULT", "PROFANITY", "THREAT"])
    return df
