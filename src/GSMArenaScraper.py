import json
from GSMArenaHelper import scrapeGSMArena


if __name__ == '__main__':
    data = []

    try:
        scrapeGSMArena(data)
    finally:
        with open(f"data/GSMArena.json", "w") as f:
            json.dump(data, f, indent=4)
