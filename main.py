import os
import json
import pandas
import click
import requests
import datetime
from enum import Enum
from dotenv import load_dotenv
from multiprocessing.dummy import Pool as ThreadPool
from alive_progress import alive_bar

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

def list_replays(target_rank: Rank, amount: int, from_date: datetime.datetime, to_date: datetime.datetime) -> list:
    ranks = {
        Rank.Bronze: {"min": "bronze-1", "max": "bronze-3"},
        Rank.Silver: {"min": "silver-1", "max": "silver-3"},
        Rank.Gold: {"min": "gold-1", "max": "gold-3"},
        Rank.Platinum: {"min": "platinum-1", "max": "platinum-3"},
        Rank.Diamond: {"min": "diamond-1", "max": "diamond-3"},
        Rank.Champion: {"min": "champion-1", "max": "champion-3"},
        Rank.GrandChampion: {"min": "grand-champion", "max": "grand-champion"}
    }

    num_replays = 0
    replays = []
    replay_ids = []

    while num_replays < amount:
        p = {
            "min-rank": ranks[target_rank]["min"],
            "max-rank": ranks[target_rank]["max"],
            "playlist": "ranked-doubles",
            "created-after": from_date,
            "created-before": to_date,
            "count": 100
        }

        r = requests.get(url=BALLCHASING_API_ENDPOINT+"/replays",
                         headers={"Authorization": os.environ["BALLCHASING_API_KEY"]}, 
                         params=p
        )
        if r.status_code != 200: r.raise_for_status()

        if DEBUG: replays += r.json()["list"]

        replay_ids += [d["id"] for d in r.json()["list"]]
        print(f"replays before cull: {len(replay_ids)}")
        replay_ids = list(dict.fromkeys(replay_ids))
        print(f"replays after cull: {len(replay_ids)}")
        num_replays = len(replay_ids)

        to_date = r.json()["list"][len(r.json()["list"]) - 1]["created"]
    
    if DEBUG:
        with open("replays.json", "w", encoding="utf-8") as f:
            json.dump(replays, f, ensure_ascii=False, indent=4)

    return replay_ids


def get_stats_from_replay(replay_id: str):
    replay_stats = {
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
        for stat_id in player["stats"].keys():
            if stat_id == "core":
                del player["stats"][stat_id]["mvp"]
            df = pandas.DataFrame.from_dict({k:[v] for k, v in player["stats"][stat_id].items()})
            replay_stats[stat_id] = pandas.concat([replay_stats[stat_id], df], ignore_index=True)
    
    return replay_stats

@click.command()
@click.option("--debug", is_flag=True)
@click.option("--rank", default=7, show_default=True, type=int)
@click.option("--replays", default=100, show_default=True, type=int)
@click.option("--from", "from_date", required=True, type=click.DateTime(['%Y-%m-%d']))
@click.option("--to", "to_date", required=True, type=click.DateTime(['%Y-%m-%d']))
def main(debug: bool, rank: int, replays: int, from_date: datetime.datetime, to_date: datetime.datetime) -> None:
    global DEBUG 
    DEBUG = debug

    ballchasing_api_tier = get_api_tier(os.environ["BALLCHASING_API_KEY"])

    replay_ids = list_replays(
        target_rank=Rank(rank),
        amount=replays,
        from_date=from_date.astimezone(datetime.timezone.utc).isoformat(timespec="microseconds"),
        to_date=to_date.astimezone(datetime.timezone.utc).isoformat(timespec="microseconds")
    )

    raw_stats = {
        "core": pandas.DataFrame(),
        "boost": pandas.DataFrame(),
        "movement": pandas.DataFrame(),
        "positioning": pandas.DataFrame(),
        "demo": pandas.DataFrame()
    }

    print(f"processing {len(replay_ids)} replays...")
    if ballchasing_api_tier != "regular":
        # TODO -- not tested ^.^
        pool = ThreadPool(4)
        results = pool.map(get_stats_from_replay, replay_ids)
    else:
        with alive_bar(len(replay_ids)) as bar:
            for replay in replay_ids[:2]:
                replay_stats = get_stats_from_replay(replay)
                for stat_id in replay_stats.keys():
                    raw_stats[stat_id] = pandas.concat([raw_stats[stat_id], replay_stats[stat_id]], ignore_index=True)
                bar()

    dir_name = f"{Rank.GrandChampion.name}-{from_date.strftime('%Y-%m-%d')}-{to_date.strftime('%Y-%m-%d')}"
    os.makedirs(dir_name, exist_ok=True)
    print(f"writing CSV data to {dir_name}...")

    with alive_bar(len(raw_stats.keys())) as bar:
        for key, value in raw_stats.items():
            value.mean().to_csv(f"{dir_name}/{key}_mean.csv", float_format="{:.3f}".format)
            bar()

if __name__ == "__main__":
    main()