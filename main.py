
from rakuten_fetcher import get_today_races
from strategy_checker import match_strategies
from notify_line import notify_if_match

races = get_today_races()

for race in races:
    if race['is_6min_before_deadline']:
        matched = match_strategies(race)
        if matched:
            notify_if_match(race, matched)
