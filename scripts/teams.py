### teams.py - Team ID Reference
# Use these opponent_id values in predict.py
# Sorted alphabetically by team name

TEAMS = {
    108: "Angels            — Angel Stadium",
    141: "Blue Jays         — Rogers Centre",
    145: "White Sox         — Rate Field",
    113: "Reds              — Great American Ball Park",
    114: "Guardians         — Progressive Field",
    115: "Rockies           — Coors Field",
    116: "Tigers            — Comerica Park",
    117: "Astros            — Daikin Park",
    118: "Royals            — Kauffman Stadium",
    110: "Orioles           — Camden Yards",
    111: "Red Sox           — Fenway Park",
    112: "Cubs              — Wrigley Field",
    121: "Mets              — Citi Field",
    147: "Yankees           — Yankee Stadium",
    133: "Athletics         — Sutter Health Park",
    143: "Phillies          — Citizens Bank Park",
    134: "Pirates           — PNC Park",
    135: "Padres            — Petco Park",
    137: "Giants            — Oracle Park",
    136: "Mariners          — T-Mobile Park",
    138: "Cardinals         — Busch Stadium",
    139: "Rays              — Steinbrenner Field",
    140: "Rangers           — Globe Life Field",
    142: "Twins             — Target Field",
    144: "Braves            — Truist Park",
    146: "Marlins           — loanDepot Park",
    119: "Dodgers           — Dodger Stadium",
    158: "Brewers           — American Family Field",
    120: "Nationals         — Nationals Park",
    109: "Diamondbacks      — Chase Field",
}

if __name__ == "__main__":
    print("\nTeam ID Reference (alphabetical)\n")
    print(f"  {'ID':<6} {'Team'}")
    print("  " + "-"*40)
    for team_id, info in sorted(TEAMS.items(), key=lambda x: x[1]):
        print(f"  {team_id:<6} {info}")
    print()
