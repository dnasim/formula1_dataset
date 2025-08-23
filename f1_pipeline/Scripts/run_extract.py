import os, sys
sys.path.append("src")

from f1.client import F1Client
from f1.loads.circuits import load_circuits
from f1.loads.constructors import load_constructors
from f1.loads.constructor_standings import load_constructor_standings

def main():
    year = int(os.getenv("F1_YEAR", "2025"))
    client = F1Client()
    load_circuits(client, year)
    load_constructors(client, year)
    load_constructor_standings(client, year)
    print("Process Completed")

if __name__ == "__main__":
    main()