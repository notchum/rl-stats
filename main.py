import os
import json
import time
import pandas
import requests
import datetime
from enum import Enum
from dotenv import load_dotenv
from multiprocessing.dummy import Pool as ThreadPool
from alive_progress import alive_bar
from nicegui import ui

# Load the environment variables
load_dotenv()

BALLCHASING_API_ENDPOINT = "https://ballchasing.com/api"

class Rank(Enum):
    Bronze = 1
    Silver = 2
    Gold = 3
    Platinum = 4
    Diamond = 5
    Champion = 6
    GrandChampion = 7

class Statistic(Enum):
    Core = 1
    Boost = 2
    Movement = 3
    Positioning = 4
    Demo = 5

def get_api_tier(api_key: str) -> str:
    r = requests.get(url=BALLCHASING_API_ENDPOINT,
                         headers={"Authorization": api_key}
        )
    if r.status_code != 200: return "noauth"

    tier = r.json()["type"]
    print(f"rate limited to {tier} tier")

    return tier

def list_replays(target_rank: Rank, amount: int) -> list:
    rank = {
        Rank.Bronze: {"min": "bronze-1", "max": "bronze-3"},
        Rank.Silver: {"min": "silver-1", "max": "silver-3"},
        Rank.Gold: {"min": "gold-1", "max": "gold-3"},
        Rank.Platinum: {"min": "platinum-1", "max": "platinum-3"},
        Rank.Diamond: {"min": "diamond-1", "max": "diamond-3"},
        Rank.Champion: {"min": "champion-1", "max": "champion-3"},
        Rank.GrandChampion: {"min": "grand-champion", "max": "grand-champion"}
    }

    replay_date_after = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=2)).isoformat()
    replay_date_before = datetime.datetime.now(datetime.timezone.utc).isoformat() 
    num_replays = 0
    replay_ids = []

    while num_replays < amount:
        p = {
            "min-rank": rank[target_rank]["min"],
            "max-rank": rank[target_rank]["max"],
            "playlist": "ranked-doubles",
            "replay-date-after": replay_date_after,
            "replay-date-before": replay_date_before
        }

        r = requests.get(url=BALLCHASING_API_ENDPOINT+"/replays",
                         headers={"Authorization": os.environ["BALLCHASING_API_KEY"]}, 
                         params=p
        )
        if r.status_code != 200: r.raise_for_status()

        replay_ids += [d["id"] for d in r.json()["list"]]
        print(f"replays before cull: {len(replay_ids)}")
        replay_ids = list(dict.fromkeys(replay_ids))
        print(f"replays after cull: {len(replay_ids)}")
        num_replays = len(replay_ids)

        replay_date_before = r.json()["list"][len(r.json()["list"]) - 1]["created"]

    return replay_ids


def get_stats_from_replay(replay_id: str):
    raw_stats = {
        "core": pandas.DataFrame(),
        "boost": pandas.DataFrame(),
        "movement": pandas.DataFrame(),
        "positioning": pandas.DataFrame(),
        "demo": pandas.DataFrame()
    }

    r = requests.get(url=BALLCHASING_API_ENDPOINT+f"/replays/{replay_id}",
                     headers={"Authorization": os.environ["BALLCHASING_API_KEY"]}
    )
    if r.status_code != 200: r.raise_for_status()

    for player in r.json()["blue"]["players"] + r.json()["orange"]["players"]:
        # with open('data.json', 'w', encoding='utf-8') as f:
        #     json.dump(player, f, ensure_ascii=False, indent=4)

        for stat_id in player["stats"].keys():
            if stat_id == "core":
                del player["stats"][stat_id]["mvp"]
            df = pandas.DataFrame.from_dict({k:[v] for k, v in player["stats"][stat_id].items()})
            raw_stats[stat_id] = pandas.concat([raw_stats[stat_id], df], ignore_index=True)
    
    return raw_stats

def main() -> None:
    ballchasing_api_tier = get_api_tier(os.environ["BALLCHASING_API_KEY"])
    


#######################################################
######                                           ######
######               USER INTERFACE              ######
######                                           ######
#######################################################

ui.dark_mode().enable()

with ui.row():
    ballchasing_api_key = ui.input(
        label="Ballchasing API key"
    )
    ui.button(
        text="VALIDATE",
        on_click=lambda: ballchasing_api_tier.set_text(get_api_tier(ballchasing_api_key.value))
    )

ballchasing_api_tier = ui.label(text="noauth")

with ui.row().bind_visibility_from(ballchasing_api_tier, target_name="text", value="regular"):
    sample_size = ui.number(
        label="Sample Size",
        min=50,
        max=1000
    )
    ui.button(
        text="RUN",
        on_click=lambda: print("fdlfjd")
    )
ui.run()

# replay_ids = list_replays(Rank.GrandChampion, 50)

# print(f"processing {len(replay_ids)} replays...")

# results = []
# if ballchasing_api_tier != "regular":
#     pool = ThreadPool(4)
#     results = pool.map(get_stats_from_replay, replay_ids)
# else:
#     with alive_bar(len(replay_ids)) as bar:
#         for replay in replay_ids:
#             replay_stats = get_stats_from_replay(replay)
#             bar()



print()
# avg = raw_stats["boost"].mean()
# print(avg)