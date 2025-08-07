
def match_strategies(race):
    o = race['odds']
    if 2.0 <= o[1] < 10.0 and o[2] < 10.0 and o[3] < 10.0 and o[4] >= 15.0:
        return '①'
    if o[1] < 2.0 and o[2] < 10.0 and o[3] < 10.0:
        return '②'
    if o[1] <= 1.5 and any(10.0 <= v <= 20.0 for k, v in o.items() if k > 1):
        return '③'
    if o[1] <= 3.0 and o[2] <= 3.0 and 6.0 <= o[3] <= 10.0 and o[4] >= 15.0:
        return '④'
    return None
